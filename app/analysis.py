import pandas as pd

def find_best_assets(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    """
    Recebe um DataFrame JÁ FILTRADO, limpa os dados inválidos e encontra os melhores ativos.
    """
    if df.empty:
        return df
    
    # **INÍCIO DA CORREÇÃO**
    # Pré-limpeza: Remove quaisquer linhas onde a 'Taxa' não pôde ser convertida para número.
    # Isso evita o erro 'must be real number, not NoneType'.
    df_cleaned = df.dropna(subset=['Taxa'])
    # **FIM DA CORREÇÃO**
    
    # A análise agora usa o DataFrame limpo (df_cleaned)
    df_sorted = df_cleaned.sort_values(by='Taxa', ascending=False)
    
    daily_assets = df_sorted[df_sorted['Liquidez_Diaria']].head(top_n)
    
    term_assets_df = df_sorted[~df_sorted['Liquidez_Diaria']]
    term_assets = term_assets_df.groupby('Ano_Vencimento').head(top_n)
    
    analysis_result = pd.concat([daily_assets, term_assets]).sort_values(
        by=['Liquidez_Diaria', 'Ano_Vencimento', 'Taxa'], 
        ascending=[False, True, False]
    )
    return analysis_result