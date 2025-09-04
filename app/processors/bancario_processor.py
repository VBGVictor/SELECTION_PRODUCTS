import pandas as pd
import re

def process(df):
    """
    Processador especializado para relatórios de Crédito Bancário.
    """
    column_map = {'produto': 'produto', 'taxa': 'taxa', 'prazo': 'prazovencimento', 'aplicaomnima': 'aplicaominima', 'roa': 'roa'}
    new_columns, used_standard_names = {}, set()
    for col in df.columns:
        if not col or pd.isna(col): continue
        normalized_col = re.sub(r'[^a-z0-9]', '', str(col).lower())
        for keyword, standard_name in column_map.items():
            if keyword in normalized_col and standard_name not in used_standard_names:
                new_columns[col] = standard_name
                used_standard_names.add(standard_name)
                break
    df.rename(columns=new_columns, inplace=True)

    if 'produto' not in df.columns:
        raise ValueError("Este não parece ser um relatório de Crédito Bancário.")

    print("INFO: Usando o processador de Crédito Bancário.")
    
    records = []
    # **INÍCIO DA CORREÇÃO**
    # 1. Pré-limpeza: Remove todas as linhas que são completamente vazias
    df_cleaned = df.dropna(how='all').reset_index(drop=True)
    
    # 2. Converte para uma estrutura mais fácil de iterar
    data_list = df_cleaned.to_dict('records')
    # **FIM DA CORREÇÃO**
    
    for i in range(len(data_list) - 1):
        current_row = data_list[i]
        produto_completo = current_row.get('produto')
        
        if pd.notna(produto_completo) and produto_completo.strip() != '':
            next_row = data_list[i+1]
            vencimento = next((str(cell) for cell in next_row.values() if re.match(r'(\d{4}-\d{2}-\d{2})', str(cell))), None)

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