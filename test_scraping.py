import asyncio
import pandas as pd
import re
import os
import json
import random
from playwright.async_api import async_playwright, TimeoutError
from bs4 import BeautifulSoup

# --- Módulo de Extração de Emissores (Validado) ---

def extract_product_and_issuer_test(full_product):
    if not isinstance(full_product, str): return 'N/A', 'N/A'
    issuer_keywords = ['Banco', 'Agibank', 'BDMG', 'genial', 'XP', 'BTG', 'Daycoval', 'C6', 'Bmg', 'FIBRA', 'Haitong', 'Master', 'Omni', 'Pine', 'Rodobens', 'Voiter', 'Digimais', 'Facta', 'Agrolend']
    pattern = r'\s?(' + '|'.join(issuer_keywords) + r'.*)'
    match = re.search(pattern, full_product, re.IGNORECASE)
    if not match: return full_product.strip(), 'N/A'
    split_index = match.start()
    produto_limpo = full_product[:split_index].strip()
    emissor = full_product[split_index:].strip().replace('No vencimento', '').replace('\xa0', ' ').strip()
    emissor = re.sub(r'\sS$', '', emissor)
    abbreviations = {"Banco BTG Pactual": "BTG Pactual", "Banco Daycoval": "Daycoval", "Banco C6 Consignado": "C6", "Banco BMG": "BMG", "Banco Agibank": "Agibank"}
    for full, abbr in abbreviations.items():
        if full in emissor:
            emissor = abbr
            break
    return produto_limpo, emissor.strip()

# --- Novo Módulo de Scraping em Fontes Oficiais ---

async def get_investor_relations_url(page, issuer_name):
    """Usa o Google para encontrar a página de Relações com Investidores."""
    search_query = f"{issuer_name} relações com investidores"
    print(f"INFO: [Google] Buscando por: '{search_query}'")
    
    try:
        await page.goto("https://www.google.com/search?q=" + search_query.replace(" ", "+"), wait_until="networkidle")
        # Pega o primeiro link de resultado orgânico que não seja anúncio
        href = await page.locator("div.g a >> nth=0").get_attribute("href")
        if href:
            print(f"SUCCESS: [Google] URL de RI encontrada: {href}")
            return href
        return None
    except Exception as e:
        print(f"WARN: [Google] Não foi possível encontrar a URL de RI para '{issuer_name}'. Erro: {e}")
        return None

async def scrape_official_site(page, ri_url, issuer_name):
    """Navega até o site de RI e tenta extrair os dados."""
    print(f"INFO: [Scraper] Acessando site oficial de RI para '{issuer_name}'...")
    summary = {}
    
    try:
        await page.goto(ri_url, wait_until="networkidle", timeout=45000)
        # Espera um pouco para o conteúdo dinâmico carregar
        await page.wait_for_timeout(3000) 
        
        body_text = await page.locator("body").inner_text()
        
        # **Lógica de Extração (Exemplo)**
        # Procura por padrões de texto comuns em sites de RI
        
        # 1. Índice de Basileia
        basileia_match = re.search(r'Basileia.*?(\d{1,2}[,.]\d{1,2}%)', body_text, re.IGNORECASE)
        if basileia_match:
            summary["Índice de Basileia"] = basileia_match.group(1)
            print(f"  -> Basileia encontrada: {summary['Índice de Basileia']}")

        # 2. Rating (Exemplo para agência S&P)
        rating_match = re.search(r'(?:S&P|Standard & Poor\'s).*?(br[A-Z]{2,3}[+-]?)', body_text, re.IGNORECASE)
        if rating_match:
            summary["Rating (S&P)"] = rating_match.group(1)
            print(f"  -> Rating encontrado: {summary['Rating (S&P)']}")
            
        return summary if summary else None

    except Exception as e:
        print(f"ERROR: [Scraper] Falha ao processar a página de RI para '{issuer_name}'. Causa: {e}")
        return None

# --- Script Principal de Teste ---

async def main_test():
    print("--- INICIANDO SCRIPT DE TESTE (V7 - FONTES OFICIAIS) ---")
    
    project_root = os.path.dirname(os.path.abspath(__file__))
    product_data_path = os.path.join(project_root, 'data', 'credito bancario.xlsx')
    output_json_path = os.path.join(project_root, 'data', 'dados_oficiais.json')

    print(f"\n[PASSO 1] Lendo arquivo de: {product_data_path}")
    df_raw = pd.read_excel(product_data_path, header=2, dtype=str).dropna(subset=['Produto'])
    
    print("\n[PASSO 2] Extraindo emissores...")
    emissores = df_raw['Produto'].apply(extract_product_and_issuer_test).apply(lambda x: x[1])
    emissores_unicos = sorted(emissores[emissores != 'N/A'].dropna().unique())
    print(f"SUCESSO: {len(emissores_unicos)} emissores encontrados: {emissores_unicos}")

    print("\n[PASSO 3] Iniciando scraping em fontes oficiais...")
    all_bank_data = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
            java_script_enabled=True,
            ignore_https_errors=True
        )
        page = await context.new_page()
        
        for issuer in emissores_unicos:
            print(f"\n--- Processando Emissor: {issuer} ---")
            ri_url = await get_investor_relations_url(page, issuer)
            if ri_url:
                data = await scrape_official_site(page, ri_url, issuer)
                if data:
                    all_bank_data[issuer] = data
            await asyncio.sleep(random.uniform(2, 4)) # Pausa para simular comportamento humano

        await browser.close()

    print("\n[PASSO 4] Exibindo e salvando os dados coletados...")
    if not all_bank_data:
        print("FALHA: Nenhum dado foi coletado. A estrutura dos sites de RI pode precisar de ajustes no parser.")
    else:
        print(json.dumps(all_bank_data, indent=4, ensure_ascii=False))
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(all_bank_data, f, ensure_ascii=False, indent=4)
        print(f"\nSUCESSO: Dados de {len(all_bank_data)} emissores salvos em '{output_json_path}'.")

    print("\n--- FIM DO SCRIPT DE TESTE ---")


if __name__ == "__main__":
    asyncio.run(main_test())