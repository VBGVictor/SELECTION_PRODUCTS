import pandas as pd
import re

def find_and_align_data(df):
    """
    Implementa a lógica de "copiar, deletar, alinhar e colar de volta" para
    corrigir o alinhamento dos dados.
    """
    possible_headers = ['produto', 'vencimento', 'rentabilidade anual', 'preço unitário']
    header_row_index = None

    # Etapa 1: Encontrar a linha do cabeçalho
    for r in range(min(10, len(df))):
        row_content_str = ' '.join(str(c) for c in df.iloc[r].values if pd.notna(c)).lower()
        if sum(h in row_content_str for h in possible_headers) >= 2:
            header_row_index = r
            break
    
    if header_row_index is None:
        raise ValueError("Linha do cabeçalho para Títulos Públicos não encontrada.")

    # Etapa 2: Copiar e guardar o cabeçalho
    header_values = df.iloc[header_row_index].dropna().tolist()

    # Etapa 3: Isolar os dados (tudo abaixo do cabeçalho)
    df_data_part = df.iloc[header_row_index + 1:].copy()

    # Etapa 4: Encontrar a primeira coluna que contém dados
    data_col_start = 0
    for c in range(min(10, len(df_data_part.columns))):
        if not df_data_part.iloc[:, c].isna().all():
            data_col_start = c
            break
            
    # Etapa 5: Alinhar os dados (remover colunas vazias à esquerda)
    df_aligned_data = df_data_part.iloc[:, data_col_start:]
    
    # Etapa 6: Colar o cabeçalho de volta
    # Garante que o número de colunas do cabeçalho corresponda ao dos dados
    num_data_cols = df_aligned_data.shape[1]
    df_final = pd.DataFrame(df_aligned_data.values, columns=header_values[:num_data_cols])
    
    return df_final


def process(df):
    """
    Processador especializado para relatórios de Títulos Públicos.
    """
    print("INFO: Usando o processador de Títulos Públicos.")
    
    try:
        df_aligned = find_and_align_data(df)
    except ValueError as e:
        # Passa o erro para o data_processor, que irá parar o processamento deste arquivo.
        raise ValueError(f"Falha ao alinhar dados de Títulos Públicos: {e}")

    df = df_aligned.copy()
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