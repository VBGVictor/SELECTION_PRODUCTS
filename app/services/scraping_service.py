import asyncio
import pandas as pd
import json
import re
import os
import random
from playwright.async_api import async_playwright, TimeoutError
from bs4 import BeautifulSoup
from app.data_processor import extract_product_and_issuer

# (As funções auxiliares como get_project_root, clean_issuer_name_for_url, etc. permanecem as mesmas)
def get_project_root():
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def clean_issuer_name_for_url(name):
    # Mapeamento para URLs específicas do bancodata.com.br
    mapping = {
        "BANCO NACIONAL DE DESENVOLVIMENTO ECONOMICO E SOCI": "bndes",
        "Agrolend SCFI": "agrolend",
        "Banco ABC": "banco-abc-brasil",
        "C6": "banco-c6",
        "Banco Randon": "banco-randon"
    }
    # Verifica se o nome exato está no mapeamento
    for key, value in mapping.items():
        if key.lower() in name.lower():
            return value
            
    # Lógica padrão para os outros casos
    name = re.sub(r'banco', '', name, flags=re.IGNORECASE).strip()
    name = name.lower()
    name = re.sub(r'[^a-z0-9\s-]', '', name)
    name = re.sub(r'\s+', '-', name)
    return name

async def fetch_bank_data_robust(page, original_name):
    """
    Tenta extrair dados de um emissor usando técnicas avançadas para evitar bloqueios.
    """
    issuer_slug = clean_issuer_name_for_url(original_name)
    url = f"https://bancodata.com.br/relatorio/{issuer_slug}/"
    print(f"INFO: [Scraper] Tentando emissor '{original_name}' na URL: {url}")
    
    try:
        # **TÉCNICA 1: Navegação mais humana**
        # 'networkidle' espera a rede ficar ociosa, simulando um usuário que espera a página carregar.
        await page.goto(url, wait_until='networkidle', timeout=30000)
        
        # **TÉCNICA 2: Espera explícita e inteligente**
        # Aguarda um elemento chave aparecer, confirmando que o conteúdo principal carregou.
        await page.wait_for_selector("div.card-body:has-text('Basileia')", timeout=15000)
        
        # **TÉCNICA 3: Extração com BeautifulSoup**
        # Passa o HTML renderizado para o BeautifulSoup, que é excelente para parsing.
        html_content = await page.content()
        soup = BeautifulSoup(html_content, 'html.parser')
        
        def get_text_by_label(label_text):
            # Procura uma div que contenha o texto do label (ex: "Basileia")
            element = soup.find('div', string=re.compile(label_text, re.IGNORECASE))
            if element:
                # O valor está na div seguinte com a classe 'fs-5'
                value_element = element.find_next_sibling('div', class_='fs-5')
                if value_element:
                    return value_element.get_text(strip=True)
            return "N/D" # Não disponível

        summary = {
            "Índice de Basileia": get_text_by_label("Basileia"),
            "Índice de Imobilização": get_text_by_label("Imobilização"),
            "Lucro Líquido (12M)": get_text_by_label("Lucro Líquido \(12M\)"),
            "Ativos Totais": get_text_by_label("Ativos Totais"),
            "Patrimônio Líquido": get_text_by_label("Patrimônio Líquido")
        }
        
        # Validação final: se todos os dados forem "N/D", considera falha.
        if all(v == "N/D" for v in summary.values()):
             print(f"WARN: [Scraper] A página para '{original_name}' carregou, mas os dados não foram encontrados no layout esperado.")
             return None

        print(f"SUCCESS: [Scraper] Dados extraídos para '{original_name}'.")
        return summary
        
    except TimeoutError:
        print(f"WARN: [Scraper] Timeout para '{original_name}'. O site pode estar bloqueando o acesso, ou a página/relatório não existe.")
        return None
    except Exception as e:
        print(f"ERROR: [Scraper] Erro inesperado ao processar '{original_name}': {e}")
        return None

async def run_scraping_async(product_data_path, bancodata_json_path):
    try:
        # 1. Ler o arquivo Excel
        df_raw = pd.read_excel(product_data_path, header=2, dtype=str)
        
        # **INÍCIO DA CORREÇÃO**
        # 2. Limpar os nomes das colunas, assim como no data_processor.py
        df_raw.dropna(how='all', axis=1, inplace=True)
        clean_columns = {col: re.sub(r'[^A-Za-z0-9]+', '', str(col).lower()) for col in df_raw.columns}
        df_raw.rename(columns=clean_columns, inplace=True)
        
        # 3. Agora, podemos acessar a coluna 'produto' com segurança
        if 'produto' not in df_raw.columns:
            print("ERROR: A coluna 'produto' não foi encontrada no arquivo Excel após a limpeza dos nomes.")
            return

        df_raw.dropna(subset=['produto'], inplace=True)
        # **FIM DA CORREÇÃO**

        emissores = df_raw['produto'].apply(extract_product_and_issuer).apply(lambda x: x[1])
        emissores_unicos = sorted(emissores[emissores != 'N/A'].dropna().unique())
        print(f"INFO: Emissores únicos encontrados para scraping: {emissores_unicos}")
    except Exception as e:
        print(f"ERROR: Falha ao ler e processar o arquivo Excel para extrair emissores: {e}")
        return

    all_bank_data = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()

        for issuer in emissores_unicos:
            data = await fetch_bank_data_robust(page, issuer)
            if data:
                all_bank_data[issuer] = data
            await asyncio.sleep(random.uniform(1, 3))

        await browser.close()

    with open(bancodata_json_path, 'w', encoding='utf-8') as f:
        json.dump(all_bank_data, f, ensure_ascii=False, indent=4)
        
    print(f"\nSUCCESS: Scraping concluído! Dados de {len(all_bank_data)} de {len(emissores_unicos)} emissores foram salvos em {bancodata_json_path}")

def run_scraping_service(project_root):
    product_path = os.path.join(project_root, 'data', 'credito bancario.xlsx')
    json_path = os.path.join(project_root, 'data', 'dados_bancodata.json')
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    if not os.path.exists(product_path):
        print(f"ERROR: [Scraping Service] O arquivo de produtos não foi encontrado em '{product_path}'. Abortando.")
        return
    asyncio.run(run_scraping_async(product_path, json_path))