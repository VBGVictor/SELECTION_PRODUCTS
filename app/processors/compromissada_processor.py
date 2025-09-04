import pandas as pd
import re

def process(df):
    """
    Processador especializado para relatórios de Compromissadas.
    """
    column_map = {
        'produto': 'produto',
        'vencimento': 'vencimento',
        'rentabilidadeanual': 'taxa',
        'ir': 'ir',
        'aplicaçãomínima': 'aplicaominima'
    }

    new_columns, used_standard_names = {}, set()
    for col in df.columns:
        if not col or pd.isna(col): continue
        normalized_col = str(col).lower()
        normalized_col = re.sub(r'[^a-z0-9]', '', normalized_col)
        for keyword, standard_name in column_map.items():
            if keyword in normalized_col and standard_name not in used_standard_names:
                # **INÍCIO DA CORREÇÃO**
                new_columns[col] = standard_name
                used_standard_names.add(standard_name)
                # **FIM DA CORREÇÃO**
                break
    df.rename(columns=new_columns, inplace=True)

    if 'produto' not in df.columns or not df['produto'].str.contains('COMPROMISSADA', case=False).any():
        raise ValueError("Este não parece ser um relatório de Compromissadas.")

    print("INFO: Usando o processador de Compromissadas.")

    records = []
    for index, row in df.iterrows():
        row_data = row.to_dict()
        produto_completo = row_data.get('produto')
        if pd.notna(produto_completo) and produto_completo.strip() != '':
            records.append({
                "Produto_Completo": produto_completo,
                "Prazo_str": "",
                "Taxa_str": row_data.get('taxa'),
                "Vencimento": row_data.get('vencimento'),
                "Aplicacao_Minima": row_data.get('aplicaominima'),
                "Roa": None,
                "IR": row_data.get('ir', 'N/A')
            })
            
    return pd.DataFrame(records)