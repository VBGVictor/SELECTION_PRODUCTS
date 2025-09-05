import pandas as pd
import re

def process(df):
    """
    Processador especializado para relatórios de Crédito Privado (CRA, CRI, etc.).
    """
    # Verificação de conteúdo para garantir que é o processador certo.
    if df.to_string().upper().count('CRA') + df.to_string().upper().count('CRI') == 0:
         raise ValueError("Arquivo não contém 'CRA' ou 'CRI'.")

    print("INFO: Usando o processador de Crédito Privado.")
    df.dropna(how='all', inplace=True)
    
    # **CORREÇÃO: As chaves agora são o nome exato do cabeçalho em minúsculas.**
    column_map = {
        'produto e ativo': 'produto', # Corresponde a "Produto e Ativo"
        'vencimento': 'vencimento',
        'rentabilidade anual': 'taxa',    # Corresponde a "Rentabilidade Anual"
        'ir': 'ir',
        'aplicação mínima': 'aplicaominima', # Corresponde a "Aplicação Mínima"
        'roa': 'roa'
    }

    new_columns = {}
    for col in df.columns:
        if not col or pd.isna(col): continue
        # Lógica simplificada: apenas converte para minúsculas e procura a correspondência exata.
        clean_col = str(col).lower().strip()
        if clean_col in column_map:
            new_columns[col] = column_map[clean_col]
            
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