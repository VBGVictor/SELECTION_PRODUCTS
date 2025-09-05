import pandas as pd
import re

def process(df):
    """Processador especializado para relatórios de Debêntures."""
    print("INFO: Usando o processador de Debêntures.")
    df.dropna(how='all', inplace=True)
    column_map = {'ativo': 'produto','vencimento': 'vencimento','rentabilidade anual': 'taxa','ir': 'ir','aplicação mínima': 'aplicaominima','roa': 'roa'}
    new_columns = {col: column_map[str(col).lower().strip()] for col in df.columns if str(col).lower().strip() in column_map}
    df.rename(columns=new_columns, inplace=True)
    records = []
    for index, row in df.iterrows():
        row_data = row.to_dict()
        produto_completo = row_data.get('produto')
        if pd.notna(produto_completo) and produto_completo.strip() != '' and pd.notna(row_data.get('taxa')):
            records.append({"Produto_Completo": produto_completo,"Prazo_str": "","Taxa_str": row_data.get('taxa'),"Vencimento": row_data.get('vencimento'),"Aplicacao_Minima": row_data.get('aplicaominima'),"Roa": row_data.get('roa'),"IR": row_data.get('ir', 'N/A')})
    return pd.DataFrame(records)