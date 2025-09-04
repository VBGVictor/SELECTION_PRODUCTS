import pandas as pd
import os
from .data_processor import process_data

# --- Variável de Cache ---
_cached_data = None

def get_data_dir():
    """Retorna o caminho para a pasta 'data'."""
    # O caminho é relativo à localização deste arquivo
    return os.path.join(os.path.dirname(__file__), '..', 'data')

def clear_caches():
    """Limpa o cache de dados para forçar uma releitura dos arquivos."""
    global _cached_data
    _cached_data = None
    print("INFO: Cache de dados limpo.")

def get_all_processed_data():
    """
    Função principal do orquestrador. Lê todos os arquivos .xlsx, os processa,
    consolida, limpa e armazena em cache.
    """
    global _cached_data
    if _cached_data is not None:
        return _cached_data

    print("INFO: Cache vazio. Processando todos os relatórios da pasta 'data'...")
    data_dir = get_data_dir()
    all_dfs = []
    
    if os.path.exists(data_dir):
        for filename in os.listdir(data_dir):
            if filename.endswith('.xlsx'):
                file_path = os.path.join(data_dir, filename)
                print(f"INFO: [Data Manager] Lendo o arquivo: {filename}")
                df = process_data(file_path)
                if not df.empty:
                    all_dfs.append(df)
    
    if all_dfs:
        # Consolida todos os DataFrames em um só
        final_df = pd.concat(all_dfs, ignore_index=True)
        # **Etapa crucial de limpeza:** remove quaisquer linhas duplicadas
        final_df.drop_duplicates(inplace=True)
        _cached_data = final_df
        print(f"INFO: Processamento concluído. {len(_cached_data)} linhas de dados consolidadas e limpas.")
    else:
        _cached_data = pd.DataFrame()
        print("WARN: Nenhum dado processável foi encontrado nos arquivos.")
        
    return _cached_data

def get_filter_options():
    """
    Retorna os dados necessários para popular os filtros da página inicial.
    """
    df = get_all_processed_data()
    if df.empty:
        return {
            "categorias": [], "anos": [], "tipos_produto": [],
            "tipos_taxa": [], "emissores": [], "tipos_ir": [],
            "loaded_files": [f for f in os.listdir(get_data_dir()) if f.endswith('.xlsx')]
        }

    # Extrai os valores únicos para cada filtro
    categorias = sorted(df['Categoria'].unique())
    anos = sorted(df[~df['Liquidez_Diaria']]['Ano_Vencimento'].unique())
    tipos_produto = sorted(df['Tipo_Produto_Base'].unique())
    tipos_taxa = sorted(df['Tipo_Taxa'].unique())
    emissores = sorted(df[df['Emissor'] != 'N/A']['Emissor'].unique())
    tipos_ir = sorted(df['IR'].unique())
    loaded_files = [f for f in os.listdir(get_data_dir()) if f.endswith('.xlsx')]

    return {
        "categorias": categorias, "anos": anos, "tipos_produto": tipos_produto,
        "tipos_taxa": tipos_taxa, "emissores": emissores, "tipos_ir": tipos_ir,
        "loaded_files": loaded_files
    }