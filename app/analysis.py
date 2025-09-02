import pandas as pd

def find_best_assets(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    if df.empty:
        return df
    
    df_sorted = df.sort_values(by='Taxa', ascending=False)
    
    daily_assets = df_sorted[df_sorted['Liquidez_Diaria']].head(top_n)
    term_assets = df_sorted[~df_sorted['Liquidez_Diaria']].groupby('Ano_Vencimento').head(top_n)
    
    analysis_result = pd.concat([daily_assets, term_assets]).sort_values(
        by=['Liquidez_Diaria', 'Ano_Vencimento', 'Taxa'], 
        ascending=[False, True, False]
    )
    return analysis_result