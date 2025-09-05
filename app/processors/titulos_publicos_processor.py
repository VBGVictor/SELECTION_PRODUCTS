import pandas as pd
import re

def process(df):
    """
    Processador especializado para relatórios de Títulos Públicos.
    O DataFrame já deve vir alinhado pelo data_processor.
    """
    print("INFO: Usando o processador de Títulos Públicos.")
    
    df.dropna(how='all', inplace=True)

    column_map = {
        'produto': 'produto',
        'vencimento': 'vencimento',
        'rentabilidade anual': 'taxa',
        'preço unitário': 'aplicaominima'
    }

    new_columns = {col: column_map[str(col).lower().strip()] for col in df.columns if str(col).lower().strip() in column_map}
    df.rename(columns=new_columns, inplace=True)

    if not all(c in df.columns for c in ['produto', 'vencimento', 'taxa', 'aplicaominima']):
        raise ValueError(f"Colunas essenciais não encontradas após renomeação. Encontradas: {df.columns.tolist()}")

    records = []
    for index, row in df.iterrows():
        row_data = row.to_dict()
        produto_completo = row_data.get('produto')
        if pd.notna(produto_completo) and str(produto_completo).strip() != '':
            produto_com_emissor = f"{produto_completo} Tesouro Nacional"
            records.append({
                "Produto_Completo": produto_com_emissor,
                "Prazo_str": "",
                "Taxa_str": row_data.get('taxa'),
                "Vencimento": row_data.get('vencimento'),
                "Aplicacao_Minima": row_data.get('aplicaominima'),
                "Roa": None,
                "IR": "Tabela Regressiva"
            })
            
    return pd.DataFrame(records)