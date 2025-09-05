import pandas as pd
import re

def process(df):
    """
    Processador especializado para relatórios de Compromissadas.
    """
    if df.to_string().upper().count('COMPROMISSADA') == 0:
         raise ValueError("Não é um relatório de Compromissadas.")
         
    print("INFO: Usando o processador de Compromissadas.")
    df.dropna(how='all', inplace=True)
    
    # **CORREÇÃO: As chaves agora são o nome exato do cabeçalho em minúsculas.**
    column_map = {
        'produto': 'produto',
        'vencimento': 'vencimento',
        'rentabilidade anual': 'taxa',
        'ir': 'ir',
        'aplicação mínima': 'aplicaominima'
    }

    new_columns = {}
    for col in df.columns:
        if not col or pd.isna(col): continue
        clean_col = str(col).lower().strip()
        if clean_col in column_map:
            new_columns[col] = column_map[clean_col]
            
    df.rename(columns=new_columns, inplace=True)

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