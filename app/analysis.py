import pandas as pd

def find_best_assets(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    if df.empty:
        return df
    
    # Prioriza os ativos sem carência, depois por liquidez diária e por fim pela taxa
    df_sorted = df.sort_values(by=['Sem_Carencia', 'Liquidez_Diaria', 'Taxa'], ascending=[False, False, False])
    
    # Captura os 'top_n' para cada categoria
    immediate_liquidity_assets = df_sorted[df_sorted['Sem_Carencia']].head(top_n)
    daily_liquidity_assets = df_sorted[df_sorted['Liquidez_Diaria'] & ~df_sorted['Sem_Carencia']].head(top_n)
    term_assets = df_sorted[~df_sorted['Liquidez_Diaria']].groupby('Ano_Vencimento').head(top_n)
    
    # Concatena os resultados e remove duplicatas, caso um ativo se encaixe em mais de uma categoria
    analysis_result = pd.concat([immediate_liquidity_assets, daily_liquidity_assets, term_assets]).drop_duplicates().sort_values(
        by=['Sem_Carencia', 'Liquidez_Diaria', 'Ano_Vencimento', 'Taxa'], 
        ascending=[False, False, True, False]
    )
    return analysis_result