import pandas as pd
import re

def process_data(file_path: str):
    try:
        df_raw = pd.read_excel(file_path, header=2, dtype=str)
    except Exception as e:
        raise ValueError(f"Não foi possível ler o arquivo Excel: {e}")

    df_raw.dropna(how='all', axis=1, inplace=True)
    clean_columns = {col: re.sub(r'[^A-Za-z0-9]+', '', str(col).lower()) for col in df_raw.columns}
    df_raw.rename(columns=clean_columns, inplace=True)

    vencimento_col_clean = 'prazovencimento'
    records = []
    for i, row in df_raw.iterrows():
        produto_completo = row.get('produto')
        if pd.notna(produto_completo) and produto_completo.strip() != '' and 'risco' not in produto_completo.lower():
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

    if not records: return pd.DataFrame()
    df = pd.DataFrame(records)

    def extract_product_and_issuer(full_product):
        issuer_keywords = ['Banco', 'Agibank', 'BDMG', 'genial', 'XP', 'BTG', 'Daycoval', 'C6', 'Bmg', 'FIBRA', 'Haitong', 'Master', 'Omni', 'Pine', 'Rodobens', 'Voiter', 'Digimais', 'Facta']
        pattern = r'\s?(' + '|'.join(issuer_keywords) + r'.*)'
        match = re.search(pattern, full_product, re.IGNORECASE)
        
        produto_limpo = full_product.replace('No vencimento', '').strip()
        emissor = 'N/A'
        if match:
            split_index = match.start()
            produto_limpo = full_product[:split_index].strip()
            emissor = full_product[split_index:].replace('No vencimento', '').strip()
            if produto_limpo.endswith(('-', '–')): produto_limpo = produto_limpo[:-1].strip()
        
        abbreviations = {"Banco BTG Pactual": "BTG Pactual", "Banco Daycoval": "Daycoval", "Banco C6 Consignado": "C6", "Banco BMG": "BMG", "Banco Agibank": "Agibank"}
        for full, abbr in abbreviations.items():
            if full in emissor: emissor = emissor.replace(full, abbr)
        return produto_limpo, emissor
    df[['Produto', 'Emissor']] = df['Produto_Completo'].apply(lambda x: pd.Series(extract_product_and_issuer(x)))
    
    def identify_liquidez_diaria(prazo_str):
        s = str(prazo_str).lower()
        return 'diaria' in s or 'diária' in s or 'd+' in s
    df['Liquidez_Diaria'] = df['Prazo_str'].apply(identify_liquidez_diaria)
    
    df['Emissor_Display'] = df['Emissor']
    def add_liquidez_details(row):
        if row['Liquidez_Diaria']:
            prazo_str = str(row['Prazo_str'])
            details_match = re.search(r'\(.*?\)|Carência.*|D\+\d+', prazo_str, re.IGNORECASE)
            detail = ""
            if details_match: detail = details_match.group(0).strip()
            if detail and detail.lower() not in ['diaria', 'diária']:
                 row['Emissor_Display'] = f"{row['Emissor']} ({detail})"
        return row
    df = df.apply(add_liquidez_details, axis=1)

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