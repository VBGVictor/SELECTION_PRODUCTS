import pandas as pd
import re
import os
# Importa todos os processadores especialistas
from .processors import bancario_processor, privado_processor, debenture_processor, compromissada_processor, titulos_publicos_processor

def find_header_row(df):
    """
    Analisa as primeiras 20 linhas de um DataFrame para encontrar o índice da linha
    que mais se parece com um cabeçalho.
    """
    possible_headers = ['produto', 'ativo', 'taxa', 'rentabilidade', 'prazo', 'vencimento', 'emissor', 'ir', 'roa']
    max_matches = 0
    header_row_index = None
    rows_to_check = min(20, df.shape[0])
    for i in range(rows_to_check):
        try:
            row_values = df.iloc[i].dropna().astype(str).str.lower().tolist()
            normalized_row = [re.sub(r'[^a-z0-9]', '', cell) for cell in row_values]
            matches = sum(1 for header in possible_headers if any(header in cell for cell in normalized_row))
            if matches > max_matches:
                max_matches = matches
                header_row_index = i
        except Exception:
            continue
    if max_matches >= 2:
        return header_row_index
    return None

def process_data(file_path: str):
    """
    Gerenciador principal que orquestra os processadores especialistas.
    """
    try:
        df_temp = pd.read_excel(file_path, header=None, dtype=str)
        header_row_index = find_header_row(df_temp)
        if header_row_index is None:
            print(f"WARN: Cabeçalho não encontrado em {os.path.basename(file_path)}. Pulando.")
            return pd.DataFrame()
        
        df_raw = pd.read_excel(file_path, header=header_row_index, dtype=str)
        df_raw.dropna(how='all', inplace=True)
        df_raw.dropna(axis=1, how='all', inplace=True)

        # Lista de todos os processadores a serem tentados, em ordem de especificidade
        processors_to_try = [
            bancario_processor.process,
            privado_processor.process,
            debenture_processor.process,
            compromissada_processor.process,
            titulos_publicos_processor.process
        ]

        processed_df = None
        for process_func in processors_to_try:
            try:
                # Tenta processar uma cópia do DF bruto
                processed_df = process_func(df_raw.copy())
                if not processed_df.empty:
                    break # Se um processador teve sucesso, para o loop
            except ValueError:
                # O processador indicou que este não é o tipo de arquivo correto, continua para o próximo
                continue
        
        if processed_df is None or processed_df.empty:
            print(f"WARN: Nenhum processador conseguiu ler o arquivo {os.path.basename(file_path)}.")
            return pd.DataFrame()
            
        df = processed_df

    except Exception as e:
        print(f"ERROR: Falha crítica ao processar {os.path.basename(file_path)}: {e}")
        return pd.DataFrame()

    if df.empty: return pd.DataFrame()
        
    # --- CAMADA FINAL DE PADRONIZAÇÃO (COMUM A TODOS OS TIPOS DE DADOS) ---
        
    df[['Produto', 'Emissor']] = df['Produto_Completo'].apply(lambda x: pd.Series(extract_product_and_issuer(x)))
    df['Tipo_Produto_Base'] = df['Produto'].apply(classify_product_type)
    df['Categoria'] = df['Tipo_Produto_Base'].apply(assign_top_level_category)
    df['IR'] = df['IR'].fillna('N/A')

    df['Liquidez_Diaria'] = df.apply(lambda row: any(k in str(row['Prazo_str']).lower() or k in str(row['Produto_Completo']).lower() for k in ['diaria', 'diária', 'd+']) or bool(re.search(r'\sS$', str(row['Prazo_str']).strip())), axis=1)
    df['Sem_Carencia'] = df.apply(lambda row: row['Liquidez_Diaria'] and not any(re.search(k, str(row['Prazo_str']).lower()) or re.search(k, str(row['Produto_Completo']).lower()) for k in ['carência', 'carencia', r'd\+']), axis=1)
    
    df['Emissor_Display'] = df['Emissor']
    df["Vencimento"] = pd.to_datetime(df["Vencimento"], errors='coerce', dayfirst=True).fillna(pd.to_datetime(df["Vencimento"], errors='coerce'))
    df.dropna(subset=["Vencimento", "Produto"], inplace=True)

    df['Tipo_Taxa'] = df['Taxa_str'].apply(lambda s: 'Pós-fixado CDI' if 'cdi' in str(s).lower() else 'Híbrido IPCA+' if 'ipca' in str(s).lower() else 'Pré-fixado' if '% a.a.' in str(s).lower() else 'Outros')
    
    def parse_money(val):
        s = str(val).replace("R$", "").replace(".", "").replace(",", ".").strip()
        try: return float(s)
        except: return None
    df["Aplicacao_Minima"] = df["Aplicacao_Minima"].apply(parse_money)
    
    def parse_percent(val):
        s = str(val).replace('%', '').strip()
        try: num = float(s.replace(",", ".")); return num / 100 if num > 1 else num
        except: return None
    df["Roa"] = df["Roa"].apply(parse_percent)
    
    def taxa_numeric(s):
        m = re.search(r"(\d+[.,]?\d*)", str(s))
        return float(m.group(1).replace(",", ".")) if m else None
    df["Taxa"] = df["Taxa_str"].apply(taxa_numeric)

    df.dropna(subset=["Taxa"], inplace=True)
    df["Ano_Vencimento"] = df["Vencimento"].dt.year.astype(int)
    
    return df

# --- FUNÇÕES AUXILIARES (USADAS POR TODOS) ---

def extract_product_and_issuer(full_product):
    if not isinstance(full_product, str): return 'N/A', 'N/A'
    
    issuer_map = {
        'BTG': 'BTG Pactual',
        'BANCO PAN': 'Banco Pan'
    }
    for code, name in issuer_map.items():
        if str(full_product).startswith(code):
            return full_product, name

    issuer_keywords = ['Banco', 'Agibank', 'BDMG', 'genial', 'XP', 'Daycoval', 'C6', 'Bmg', 'FIBRA', 'Haitong', 'Master', 'Omni', 'Pine', 'Rodobens', 'Voiter', 'Digimais', 'Facta', 'Agrolend', 'Tesouro Nacional']
    pattern = r'\s?(' + '|'.join(issuer_keywords) + r'.*)'
    match = re.search(pattern, full_product, re.IGNORECASE)
    produto_limpo, emissor = full_product.replace('No vencimento', '').strip(), 'N/A'
    if match:
        split_index = match.start()
        produto_limpo = full_product[:split_index].strip()
        emissor = full_product[split_index:].replace('No vencimento', '').strip()
        if produto_limpo.endswith(('-', '–')):
            produto_limpo = produto_limpo[:-1].strip()
    abbreviations = {"Banco BTG Pactual": "BTG Pactual", "Banco Daycoval": "Daycoval", "Banco C6 Consignado": "C6", "Banco BMG": "BMG", "Banco Agibank": "Agibank"}
    for full, abbr in abbreviations.items():
        if full in emissor: emissor = abbr
    emissor = re.sub(r'Diária.*', '', emissor).strip()
    return produto_limpo, emissor

def classify_product_type(produto_str):
    s = str(produto_str).upper()
    # Ordem importa: Tesouro Direto é um tipo de Título Público
    for p_type in ['TESOURO DIRETO', 'LCA', 'LCI', 'CDB', 'LF', 'CRA', 'CRI', 'CDCA', 'DEBENTURE', 'COMPROMISSADA', 'TITULO PUBLICO']:
        search_term = p_type.replace('E', '[EÊ]')
        if re.search(r'\b' + search_term + r'\b', s):
            if 'DEBENTURE' in p_type: return 'DEBENTURE'
            if 'TITULO' in p_type: return 'TITULO PUBLICO'
            return p_type
    return 'Outros'

def assign_top_level_category(product_type):
    credito_bancario = ['LCA', 'LCI', 'CDB', 'LF']
    credito_privado = ['CRA', 'CRI', 'CDCA', 'DEBENTURE', 'COMPROMISSADA']
    titulos_publicos = ['TITULO PUBLICO', 'TESOURO DIRETO']
    if product_type in credito_bancario: return 'Crédito Bancário'
    if product_type in credito_privado: return 'Crédito Privado'
    if product_type in titulos_publicos: return 'Títulos Públicos/Tesouro'
    return 'Outros'