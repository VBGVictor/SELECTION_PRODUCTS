import pandas as pd
import re

def process(df):
    """
    Processador especializado para relatórios de Crédito Bancário.
    """
    # **CORREÇÃO: Lógica de autoidentificação específica.**
    # Ele só se identifica se encontrar produtos bancários e não for de outro tipo.
    content = df.to_string().upper()
    is_bancario_candidate = 'LCA' in content or 'LCI' in content or 'CDB' in content or 'LF' in content
    
    if not (is_bancario_candidate):
        raise ValueError("Este não parece ser um relatório de Crédito Bancário.")

    print("INFO: Usando o processador de Crédito Bancário.")
    
    column_map = {'produto': 'produto', 'taxa': 'taxa', 'prazo/vencimento': 'prazovencimento', 'aplicação mínima': 'aplicaominima', 'roa': 'roa'}

    new_columns = {}
    for col in df.columns:
        if not col or pd.isna(col): continue
        clean_col = str(col).lower().strip()
        if clean_col in column_map:
            new_columns[col] = column_map[clean_col]

    df.rename(columns=new_columns, inplace=True)

    if 'produto' not in df.columns:
        raise ValueError("Coluna 'produto' não encontrada após renomeação.")

    records = []
    df_cleaned = df.reset_index(drop=True)
    data_list = df_cleaned.to_dict('records')
    
    for i in range(len(data_list) - 1):
        current_row = data_list[i]
        produto_completo = current_row.get('produto')
        
        if pd.notna(produto_completo) and produto_completo.strip() != '':
            next_row = data_list[i+1]
            vencimento = next((str(cell).split(' ')[0] for cell in next_row.values() if isinstance(cell, str) and re.match(r'^\d{4}-\d{2}-\d{2}', cell)), None)

            if vencimento:
                records.append({
                    "Produto_Completo": produto_completo,
                    "Prazo_str": current_row.get('prazovencimento', ''),
                    "Taxa_str": current_row.get('taxa'),
                    "Vencimento": vencimento,
                    "Aplicacao_Minima": current_row.get('aplicaominima'),
                    "Roa": current_row.get('roa'),
                    "IR": "Isento"
                })

    return pd.DataFrame(records)