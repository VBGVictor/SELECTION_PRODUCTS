from fpdf import FPDF
from datetime import datetime
import pandas as pd
import os

class PDF(FPDF):
    def __init__(self, logo_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logo_path = logo_path
        self.set_auto_page_break(auto=True, margin=15)

    def header(self):
        ethimos_blue_dark = (0, 32, 96)
        if os.path.exists(self.logo_path):
            self.image(self.logo_path, 10, 8, 50)
        else:
            self.set_font("Arial", "B", 16); self.set_text_color(*ethimos_blue_dark)
            self.cell(0, 10, "Ethimos Investimentos", 0, 1, "L")
        self.set_font("Arial", "B", 18); self.set_text_color(*ethimos_blue_dark)
        self.cell(0, 20, "Relatório de Ativos de Renda Fixa", 0, 1, "C")
        self.ln(5)

    def footer(self):
        self.set_y(-15); self.set_font("Arial", "I", 8); self.set_text_color(100, 100, 100)
        self.cell(0, 10, f"Página {self.page_no()}", 0, 0, "C")
        self.set_x(-75)
        self.cell(0, 10, f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}", 0, 0, "R")

def create_pdf_report(data: pd.DataFrame, include_roa: bool, logo_path: str):
    pdf = PDF(logo_path=logo_path, orientation='L', unit='mm', format='A4')
    pdf.add_page()

    ethimos_blue_dark = (0, 32, 96); ethimos_blue_medium = (0, 51, 153)

    if include_roa:
        col_widths = {"Produto": 55, "Emissor": 55, "Vencimento": 25, "Taxa": 30, "Apl. Mínima": 35, "ROA": 25}
    else:
        col_widths = {"Produto": 70, "Emissor": 70, "Vencimento": 30, "Taxa": 35, "Apl. Mínima": 40}

    def render_table(df_group, is_advisor):
        pdf.set_font("Arial", "B", 9); pdf.set_fill_color(*ethimos_blue_medium); pdf.set_text_color(255, 255, 255)
        pdf.cell(col_widths["Produto"], 8, "Produto", 0, 0, "C", fill=True)
        pdf.cell(col_widths["Emissor"], 8, "Emissor", 0, 0, "C", fill=True)
        pdf.cell(col_widths["Vencimento"], 8, "Vencimento", 0, 0, "C", fill=True)
        pdf.cell(col_widths["Taxa"], 8, "Taxa", 0, 0, "C", fill=True)
        pdf.cell(col_widths["Apl. Mínima"], 8, "Apl. Mínima", 0, 0, "C", fill=True)
        if is_advisor: pdf.cell(col_widths["ROA"], 8, "ROA (%)", 0, 0, "C", fill=True)
        pdf.ln()

        pdf.set_font("Arial", "", 8); pdf.set_text_color(0, 0, 0)
        for _, row in df_group.iterrows():
            pdf.cell(col_widths["Produto"], 8, str(row["Produto"])[:35], 1, 0, "L")
            pdf.cell(col_widths["Emissor"], 8, str(row["Emissor_Display"])[:35], 1, 0, "L")
            pdf.cell(col_widths["Vencimento"], 8, row["Vencimento"].strftime("%d/%m/%Y"), 1, 0, "C")
            pdf.cell(col_widths["Taxa"], 8, str(row["Taxa_str"]), 1, 0, "C")
            app_min_str = f"R$ {row['Aplicacao_Minima']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if pd.notna(row['Aplicacao_Minima']) else ""
            pdf.cell(col_widths["Apl. Mínima"], 8, app_min_str, 1, 0, "R")
            if is_advisor:
                roa_percent = f"{row['Roa']*100:.2f}%".replace(".", ",") if pd.notna(row['Roa']) else ""
                pdf.cell(col_widths["ROA"], 8, roa_percent, 1, 0, "C")
            pdf.ln()

    liquidez_imediata_assets = data[data.Sem_Carencia == True]
    liquidez_diaria_assets = data[(data.Liquidez_Diaria == True) & (data.Sem_Carencia == False)]
    prazo_assets = data[data.Liquidez_Diaria == False]
    
    if not liquidez_imediata_assets.empty:
        pdf.ln(5); pdf.set_font("Arial", "B", 14); pdf.set_text_color(*ethimos_blue_dark)
        pdf.cell(0, 12, "Liquidez Imediata (sem carência)", 0, 1, "L"); pdf.ln(2)
        render_table(liquidez_imediata_assets, include_roa)

    if not liquidez_diaria_assets.empty:
        pdf.ln(5); pdf.set_font("Arial", "B", 14); pdf.set_text_color(*ethimos_blue_dark)
        pdf.cell(0, 12, "Ativos com Liquidez Diária", 0, 1, "L"); pdf.ln(2)
        render_table(liquidez_diaria_assets, include_roa)

    if not prazo_assets.empty:
        for year, group in prazo_assets.groupby('Ano_Vencimento'):
            pdf.ln(5); pdf.set_font("Arial", "B", 14); pdf.set_text_color(*ethimos_blue_dark)
            pdf.cell(0, 12, f"Ano de Vencimento: {int(year)}", 0, 1, "L"); pdf.ln(2)
            render_table(group, include_roa)

    return pdf.output(dest="S").encode("latin-1")

def create_detailed_pdf_report(data: pd.DataFrame, bancodata: dict, logo_path: str):
    pdf = PDF(logo_path=logo_path, orientation='P', unit='mm', format='A4') # Orientação Retrato para melhor layout
    pdf.add_page()
    
    ethimos_blue_dark = (0, 32, 96)
    ethimos_blue_medium = (0, 51, 153)
    
    col_widths = {"Produto": 70, "Vencimento": 25, "Taxa": 30, "Apl. Mínima": 35, "ROA": 25}

    def render_summary(issuer_name, issuer_data):
        pdf.set_font("Arial", "B", 11)
        pdf.set_fill_color(240, 240, 240)
        pdf.set_text_color(*ethimos_blue_dark)
        pdf.multi_cell(0, 7, f"Resumo do Emissor: {issuer_name}", border=1, align="C", fill=True)
        
        pdf.set_font("Arial", "", 9)
        pdf.set_text_color(0,0,0)
        summary_text = ""
        for key, value in issuer_data.items():
            summary_text += f"      •  {key}: {value}\n"
        pdf.multi_cell(0, 6, summary_text.strip(), border="LRB")
        pdf.ln(2)

    def render_table(df_group):
        pdf.set_font("Arial", "B", 9)
        pdf.set_fill_color(*ethimos_blue_medium)
        pdf.set_text_color(255, 255, 255)
        for header, width in col_widths.items():
            pdf.cell(width, 8, header, 0, 0, "C", fill=True)
        pdf.ln()

        pdf.set_font("Arial", "", 8)
        pdf.set_text_color(0, 0, 0)
        for _, row in df_group.iterrows():
            pdf.cell(col_widths["Produto"], 8, str(row["Produto"])[:40], 1, 0, "L")
            pdf.cell(col_widths["Vencimento"], 8, row["Vencimento"].strftime("%d/%m/%Y"), 1, 0, "C")
            pdf.cell(col_widths["Taxa"], 8, str(row["Taxa_str"]), 1, 0, "C")
            app_min_str = f"R$ {row['Aplicacao_Minima']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if pd.notna(row['Aplicacao_Minima']) else ""
            pdf.cell(col_widths["Apl. Mínima"], 8, app_min_str, 1, 0, "R")
            roa_percent = f"{row['Roa']*100:.2f}%".replace(".", ",") if pd.notna(row['Roa']) else ""
            pdf.cell(col_widths["ROA"], 8, roa_percent, 1, 0, "C")
            pdf.ln()

    # Agrupa primeiro por ano e depois por emissor
    for year, year_group in data.groupby('Ano_Vencimento'):
        pdf.ln(5)
        pdf.set_font("Arial", "B", 16)
        pdf.set_text_color(*ethimos_blue_dark)
        pdf.cell(0, 12, f"Ano de Vencimento: {int(year)}", 0, 1, "L")
        
        for issuer, issuer_group in year_group.groupby('Emissor'):
            pdf.ln(3)
            issuer_data = bancodata.get(issuer) # Pega os dados do banco do JSON
            if issuer_data:
                render_summary(issuer, issuer_data)
            else:
                pdf.set_font("Arial", "B", 11)
                pdf.set_text_color(*ethimos_blue_dark)
                pdf.cell(0, 8, f"Emissor: {issuer}", 0, 1, "L")

            render_table(issuer_group)

    return pdf.output(dest="S").encode("latin-1")