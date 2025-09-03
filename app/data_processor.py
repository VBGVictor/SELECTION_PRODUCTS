import pandas as pd
import re

def find_header_row(df):
    """
    Analisa as primeiras 10 linhas de um DataFrame para encontrar a linha do cabeçalho,
    procurando por colunas-chave.
    """
    # Lista de possíveis nomes para colunas essenciais (já normalizados)
    possible_headers = ['produto', 'risco', 'taxa', 'prazo', 'vencimento']
    
    for i, row in df.head(10).iterrows():
        try:
            # Normaliza os valores da linha para a busca (minúsculas, sem espaços)
            normalized_row = [re.sub(r'[^a-z0-9]', '', str(cell).lower()) for cell in row]
            
            # Conta quantas colunas essenciais estão presentes na linha
            matches = sum(1 for header in possible_headers if any(header in cell for cell in normalized_row))
            
            # Se encontrarmos pelo menos 3 colunas-chave, consideramos que é o cabeçalho
            if matches >= 3:
                return i
        except:
            continue
            
    return None

def extract_product_and_issuer(full_product):
    if not isinstance(full_product, str):
        return 'N/A', 'N/A'

    issuer_keywords = ['Banco', 'Agibank', 'BDMG', 'genial', 'XP', 'BTG', 'Daycoval', 'C6', 'Bmg', 'FIBRA', 'Haitong', 'Master', 'Omni', 'Pine', 'Rodobens', 'Voiter', 'Digimais', 'Facta', 'Agrolend']
    pattern = r'\s?(' + '|'.join(issuer_keywords) + r'.*)'
    match = re.search(pattern, full_product, re.IGNORECASE)
    
    produto_limpo = full_product.replace('No vencimento', '').strip()
    emissor = 'N/A'
    
    if match:
        split_index = match.start()
        produto_limpo = full_product[:split_index].strip()
        emissor = full_product[split_index:].replace('No vencimento', '').strip()
        if produto_limpo.endswith(('-', '–')):
            produto_limpo = produto_limpo[:-1].strip()
    
    abbreviations = {"Banco BTG Pactual": "BTG Pactual", "Banco Daycoval": "Daycoval", "Banco C6 Consignado": "C6", "Banco BMG": "BMG", "Banco Agibank": "Agibank"}
    for full, abbr in abbreviations.items():
        if full in emissor:
            emissor = emissor.replace(full, abbr)
            
    emissor = re.sub(r'Diária.*', '', emissor).strip()
    return produto_limpo, emissor

def process_data(file_path: str):
    try:
        # **INÍCIO DA CORREÇÃO**
        # 1. Lê o arquivo sem assumir um cabeçalho para poder encontrá-lo
        df_temp = pd.read_excel(file_path, header=None, dtype=str)
        
        # 2. Encontra a linha correta do cabeçalho dinamicamente
        header_row_index = find_header_row(df_temp)
        
        if header_row_index is None:
            raise ValueError("Não foi possível encontrar o cabeçalho da tabela. Verifique se o arquivo Excel contém colunas como 'Produto', 'Taxa', etc., nas primeiras 10 linhas.")

        # 3. Lê o arquivo novamente, desta vez usando a linha de cabeçalho correta
        df_raw = pd.read_excel(file_path, header=header_row_index, dtype=str)
        # **FIM DA CORREÇÃO**

    except Exception as e:
        raise ValueError(f"Erro ao processar o arquivo Excel: {e}")

    df_raw.dropna(how='all', axis=1, inplace=True)
    clean_columns = {col: re.sub(r'[^A-Za-z0-9]+', '', str(col).lower()) for col in df_raw.columns}
    df_raw.rename(columns=clean_columns, inplace=True)

    if 'produto' not in df_raw.columns:
        raise ValueError("A coluna 'Produto' é obrigatória e não foi encontrada após a limpeza dos cabeçalhos. Verifique o seu arquivo Excel.")

    vencimento_col_clean = 'prazovencimento'
    records = []
    for i, row in df_raw.iterrows():
        produto_completo = row.get('produto')
        if pd.notna(produto_completo) and produto_completo.strip() != '' and 'risco' not in str(produto_completo).lower():
            if i + 1 < len(df_raw):
                next_row = df_raw.iloc[i+1]
                vencimento = next_row.get(vencimento_col_clean)
                if pd.notna(vencimento):
                    records.append({
                        "Produto_Completo": produto_completo,
                        "Prazo_str": row.get(vencimento_col_clean, ''),
                        "Taxa_str": row.get('taxa'),
                        "Vencimento": vencimento,
                        "Aplicacao_Minima": row.get('aplicaomnima'),
                        "Roa": row.get('roa')
                    })

    if not records: 
        return pd.DataFrame()
        
    df = pd.DataFrame(records)
    df[['Produto', 'Emissor']] = df['Produto_Completo'].apply(lambda x: pd.Series(extract_product_and_issuer(x)))
    
    def identify_liquidez_diaria(row):
        prazo_str = str(row['Prazo_str']).lower()
        produto_str = str(row['Produto_Completo']).lower()
        liquidity_keywords = ['diaria', 'diária', 'd+']
        if any(keyword in prazo_str for keyword in liquidity_keywords) or \
           any(keyword in produto_str for keyword in liquidity_keywords) or \
           re.search(r'\sS$', str(row['Prazo_str']).strip()):
            return True
        return False

    df['Liquidez_Diaria'] = df.apply(identify_liquidez_diaria, axis=1)

    def identify_sem_carencia(row):
        if not row['Liquidez_Diaria']:
            return False
        
        prazo_str = str(row['Prazo_str']).lower()
        produto_str = str(row['Produto_Completo']).lower()
        carencia_keywords = ['carência', 'carencia', r'd\+']
        
        if any(re.search(keyword, prazo_str) for keyword in carencia_keywords) or \
           any(re.search(keyword, produto_str) for keyword in carencia_keywords):
            return False
            
        return True

    df['Sem_Carencia'] = df.apply(identify_sem_carencia, axis=1)
    
    df['Emissor_Display'] = df['Emissor']

    df["Vencimento"] = pd.to_datetime(df["Vencimento"], errors="coerce")
    df.dropna(subset=["Vencimento", "Produto"], inplace=True)

    def classify_rate_type(taxa_str):
        s = str(taxa_str).lower();
        if 'cdi' in s: return 'Pós-fixado CDI'
        if 'ipca' in s: return 'Híbrido IPCA+'
        if '% a.a.' in s: return 'Pré-fixado'
        return 'Outros'
    df['Tipo_Taxa'] = df['Taxa_str'].apply(classify_rate_type)
    
    def classify_product_type(produto_str):
        s = str(produto_str).upper();
        for p_type in ['LCA', 'LCI', 'CDB', 'CRA', 'CRI', 'LF']:
            if re.search(r'\b' + p_type + r'\b', s): return p_type
        return 'Outros'
    df['Tipo_Produto_Base'] = df['Produto'].apply(classify_product_type)
    
    def parse_money(val):
        s = str(val).replace("R$", "").replace(".", "").replace(",", ".").strip();
        try: return float(s)
        except: return None
    df["Aplicacao_Minima"] = df["Aplicacao_Minima"].apply(parse_money)
    
    def parse_percent(val):
        s = str(val).replace('%', '').strip();
        try: num = float(s.replace(",", ".")); return num / 100 if num > 1 else num
        except: return None
    df["Roa"] = df["Roa"].apply(parse_percent)
    
    def taxa_numeric(s):
        m = re.search(r"(\d+[.,]?\d*)", str(s));
        return float(m.group(1).replace(",", ".")) if m else None
    df["Taxa"] = df["Taxa_str"].apply(taxa_numeric)

    df.dropna(subset=["Taxa"], inplace=True)
    df["Ano_Vencimento"] = df["Vencimento"].dt.year.astype(int)
    
    return df