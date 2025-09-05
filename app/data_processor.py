import pandas as pd
import re
import os
# Importa todos os processadores especialistas
from .processors import bancario_processor, privado_processor, debenture_processor, compromissada_processor, titulos_publicos_processor

def find_data_start_and_keywords(df):
    """
    Analisa as primeiras 10 linhas para encontrar o cabeçalho e também "espia"
    as 5 primeiras linhas de dados para extrair palavras-chave de produtos.
    """
    possible_headers = ['produto', 'ativo', 'taxa', 'rentabilidade', 'prazo', 'vencimento', 'risco', 'preçounitário']
    product_keywords = ['compromissada', 'debenture', 's.a', 's/a', 'cra', 'cri', 'cdca', 'tesouro', 'lft', 'ltn', 'ntn', 'lca', 'lci', 'cdb', 'lf']
    
    header_row_index = None
    header_keywords = []

    rows_to_check = min(10, df.shape[0])
    for r in range(rows_to_check):
        row_values = df.iloc[r].values
        matches = sum(any(header in str(cell).lower() for header in possible_headers) for cell in row_values if pd.notna(cell))
        if matches >= 2:
            header_row_index = r
            for cell in row_values:
                if pd.notna(cell):
                    cell_str = str(cell).lower()
                    for keyword in possible_headers:
                        if keyword in cell_str:
                            header_keywords.append(keyword)
            break

    if header_row_index is None:
        return None, []

    data_keywords = []
    rows_to_scan = min(header_row_index + 6, df.shape[0])
    for r in range(header_row_index + 1, rows_to_scan):
        row_str = ' '.join(df.iloc[r].astype(str).values).lower()
        for keyword in product_keywords:
            if keyword in row_str:
                data_keywords.append(keyword)

    if 's.a' in data_keywords or 's/a' in data_keywords:
        data_keywords.append('debenture')

    final_keywords = list(set(data_keywords + header_keywords))
    return header_row_index, final_keywords

def process_data(file_path: str):
    """
    Gerenciador principal com lógica de seleção hierárquica e forçada.
    """
    try:
        df_temp = pd.read_excel(file_path, header=None, dtype=str)
        header_row_index, keywords = find_data_start_and_keywords(df_temp)

        if header_row_index is None:
            print(f"WARN: Cabeçalho não identificado em {os.path.basename(file_path)}. Pulando.")
            return pd.DataFrame()
            
        df_raw = pd.read_excel(file_path, header=header_row_index, dtype=str)
        df_raw.dropna(axis=1, how='all', inplace=True)
        
        print(f"INFO: Arquivo '{os.path.basename(file_path)}' lido. Palavras-chave de identificação: {keywords}")

        # **INÍCIO DA LÓGICA DE SELEÇÃO HIERÁRQUICA**
        selected_processor = None
        if any(k in keywords for k in ['cdb', 'lci', 'lca']):
            selected_processor = bancario_processor.process
        elif any(k in keywords for k in ['compromissada']):
            selected_processor = compromissada_processor.process
        elif any(k in keywords for k in ['cra', 'cri', 'cdca']):
            selected_processor = privado_processor.process
        elif any(k in keywords for k in ['debenture']):
            selected_processor = debenture_processor.process
        elif any(k in keywords for k in ['tesouro', 'lft', 'ltn', 'ntn', 'preçounitário']):
            selected_processor = titulos_publicos_processor.process
        else:
            # Fallback: se não for nenhum dos tipos específicos, é bancário.
            if selected_processor is None:
                selected_processor = bancario_processor.process
        # **FIM DA LÓGICA DE SELEÇÃO**

        processed_df = selected_processor(df_raw.copy())
        print(f"INFO: Arquivo processado por: {selected_processor.__module__}")

        if processed_df is None or processed_df.empty:
            print(f"WARN: O processador não retornou dados processáveis.")
            return pd.DataFrame()
            
        df = processed_df

    except Exception as e:
        print(f"ERROR: Falha crítica ao processar {os.path.basename(file_path)}: {e}")
        return pd.DataFrame()

    if df.empty: return pd.DataFrame()
        
    df[['Produto', 'Emissor']] = df['Produto_Completo'].apply(lambda x: pd.Series(extract_product_and_issuer(x)))
    df['Tipo_Produto_Base'] = df['Produto'].apply(classify_product_type)
    df['Categoria'] = df['Tipo_Produto_Base'].apply(assign_top_level_category)
    df['IR'] = df['IR'].fillna('N/A')
    df['Liquidez_Diaria'] = df.apply(lambda row: any(k in str(row['Prazo_str']).lower() or k in str(row['Produto_Completo']).lower() for k in ['diaria', 'diária', 'd+']) or bool(re.search(r'\sS$', str(row['Prazo_str']).strip())), axis=1)
    df['Sem_Carencia'] = df.apply(lambda row: row['Liquidez_Diaria'] and not any(re.search(k, str(row['Prazo_str']).lower()) or re.search(k, str(row['Produto_Completo']).lower()) for k in ['carência', 'carencia', r'd\+']), axis=1)
    df['Emissor_Display'] = df['Emissor']
    df["Vencimento"] = pd.to_datetime(df["Vencimento"], errors='coerce', dayfirst=False).fillna(pd.to_datetime(df["Vencimento"], errors='coerce', dayfirst=True))
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

# --- Funções Auxiliares ---
def extract_product_and_issuer(full_product):
    if not isinstance(full_product, str): return 'N/A', 'N/A'
    produto_limpo, emissor = 'N/A', 'N/A'
    match_debenture = re.search(r'^(.*?S\.(?:A|A\.))\s*([A-Z0-9]+)$', full_product, re.IGNORECASE)
    if match_debenture:
        emissor = match_debenture.group(1).strip()
        produto_limpo = match_debenture.group(2).strip()
        return produto_limpo, emissor
    match_privado = re.search(r'^(CRA|CRI|CDCA)\s*-?\s*([A-Z\s\d\'.()]+?)\s*([A-Z]{2,}\d{2,}.*)', full_product, re.IGNORECASE)
    if match_privado:
        produto_limpo = match_privado.group(1).strip().upper()
        emissor = match_privado.group(2).strip()
        if len(emissor) <= 2: emissor = full_product
        return produto_limpo, emissor
    issuer_keywords = ['Banco', 'Agibank', 'BDMG', 'genial', 'XP', 'Daycoval', 'C6', 'Bmg', 'FIBRA', 'Haitong', 'Master', 'Omni', 'Pine', 'Rodobens', 'Voiter', 'Digimais', 'Facta', 'Agrolend', 'Tesouro Nacional']
    pattern = r'^(.*?)\s?(' + '|'.join(issuer_keywords) + r'.*)'
    match_bancario = re.search(pattern, full_product, re.IGNORECASE)
    if match_bancario:
        produto_limpo = match_bancario.group(1).strip()
        emissor = match_bancario.group(2).strip()
        abbreviations = {"Banco BTG Pactual": "BTG Pactual", "Banco Daycoval": "Daycoval", "Banco C6 Consignado": "C6", "Banco BMG": "BMG", "Banco Agibank": "Agibank"}
        for full, abbr in abbreviations.items():
            if full in emissor: emissor = abbr
        emissor = re.sub(r'Diária.*', '', emissor).strip()
        return produto_limpo, emissor
    type_match = re.search(r'^(CRA|CRI|CDCA|DEBENTURE|LCA|LCI|CDB|LF)', full_product, re.IGNORECASE)
    if type_match:
        produto_limpo = type_match.group(1).upper()
    else:
        produto_limpo = full_product
    emissor = full_product
    return produto_limpo, emissor

def classify_product_type(produto_str):
    s = str(produto_str).upper()
    if 'S.A' in s or 'S/A' in s: return 'DEBENTURE'
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