import pandas as pd
import re

def find_header_row(df):
    """
    Analisa as primeiras 10 linhas para encontrar o índice da linha
    que mais se parece com um cabeçalho.
    """
    possible_headers = ['produto', 'ativo', 'taxa', 'rentabilidade', 'prazo', 'vencimento', 'risco']
    rows_to_check = min(10, df.shape[0])
    
    for r in range(rows_to_check):
        row_values = df.iloc[r].values
        matches = sum(any(header in str(cell).lower() for header in possible_headers) for cell in row_values if pd.notna(cell))
        if matches >= 2:
            return r 
    return None

def process(df):
    """
    Processador especializado para relatórios de Crédito Privado (CRA, CRI, etc.).
    Agora ele é responsável por encontrar seu próprio cabeçalho e validar o arquivo.
    """
    # 1. Validação de conteúdo: Verifica se 'CRA' ou 'CRI' existem no arquivo.
    # Se não encontrar, levanta um erro para o data_processor tentar o próximo da lista.
    if df.to_string().upper().count('CRA') + df.to_string().upper().count('CRI') == 0:
         raise ValueError("Arquivo não contém 'CRA' ou 'CRI'.")

    print("INFO: Usando o processador de Crédito Privado.")

    # 2. Encontra o cabeçalho correto dentro deste processador.
    # Para isso, ele precisa ler o DataFrame original como se não tivesse cabeçalho.
    # A forma mais segura é trabalhar com os valores numpy.
    header_row_index = find_header_row(pd.DataFrame(df.values))
    
    if header_row_index is None:
        raise ValueError("Cabeçalho do relatório de Crédito Privado não encontrado.")

    # 3. Cria um novo DataFrame a partir da linha do cabeçalho encontrada.
    new_header = df.iloc[header_row_index]
    df = df[header_row_index + 1:]
    df.columns = new_header
    
    df.dropna(how='all', inplace=True)
    
    column_map = {
        'produtoeativo': 'produto', 'ativo': 'produto', 'produto': 'produto',
        'vencimento': 'vencimento',
        'rentabilidadeanual': 'taxa', 'taxa': 'taxa',
        'ir': 'ir',
        'aplicaominima': 'aplicaominima',
        'roa': 'roa'
    }

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

    records = []
    for index, row in df.iterrows():
        row_data = row.to_dict()
        produto_completo = row_data.get('produto')
        
        if pd.notna(produto_completo) and produto_completo.strip() != '' and pd.notna(row_data.get('taxa')):
            records.append({
                "Produto_Completo": produto_completo,
                "Prazo_str": "",
                "Taxa_str": row_data.get('taxa'),
                "Vencimento": row_data.get('vencimento'),
                "Aplicacao_Minima": row_data.get('aplicaominima'),
                "Roa": row_data.get('roa'),
                "IR": row_data.get('ir', 'N/A')
            })
            
    return pd.DataFrame(records)

if __name__ == "__main__":
    # Teste rápido
    data = pd.read_excel('data/cra-cri.xlsx')
    df_test1 = find_header_row(data)
    print("Início dos dados na linha:", df_test1)
    df_test = pd.DataFrame(data)
    df_test1 = pd.DataFrame(data)
    print('teste 1', df_test)
    processed_df1 = process(df_test1)
    print(processed_df1)
    processed_df = process(df_test)
    print(processed_df)