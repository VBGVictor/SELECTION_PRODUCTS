from flask import Blueprint, render_template, make_response, request, redirect, url_for, Response, flash
from .data_processor import process_data
from .analysis import find_best_assets
from .pdf_generator import create_pdf_report
import os
import io
import pandas as pd

main_bp = Blueprint('main', __name__)

_cached_data = {} 

def get_data_dir():
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')

def get_base_path():
    return os.path.dirname(os.path.abspath(__file__))

def clear_caches():
    global _cached_data
    _cached_data = {}
    print("INFO: Cache de dados limpo.")

def get_report_data(filename):
    global _cached_data
    if filename in _cached_data:
        return _cached_data[filename]

    file_path = os.path.join(get_data_dir(), filename)
    if os.path.exists(file_path):
        print(f"INFO: Processando o arquivo do zero: {filename}")
        df = process_data(file_path)
        _cached_data[filename] = df
        return df
    return pd.DataFrame()

def get_available_reports():
    data_dir = get_data_dir()
    if os.path.exists(data_dir):
        return sorted([f for f in os.listdir(data_dir) if f.endswith('.xlsx')])
    return []

@main_bp.route('/', methods=['GET'])
def index():
    try:
        available_reports = get_available_reports()
        active_report = request.args.get('report')

        if not active_report and available_reports:
            active_report = available_reports[0]
        
        df = pd.DataFrame()
        filter_options = {}

        if active_report:
            df = get_report_data(active_report)
            if not df.empty:
                filter_options = {
                    "anos": sorted(df[~df['Liquidez_Diaria']]['Ano_Vencimento'].unique()),
                    "tipos_produto": sorted(df['Tipo_Produto_Base'].unique()),
                    "tipos_taxa": sorted(df['Tipo_Taxa'].unique()),
                    "emissores": sorted(df[df['Emissor'] != 'N/A']['Emissor'].unique()),
                    "tipos_ir": sorted(df['IR'].unique())
                }
        
        if df.empty and active_report:
             flash(f'O relatório "{active_report}" não pôde ser processado ou não contém dados válidos. Verifique o arquivo.', 'error')

        return render_template('index.html', 
                               available_reports=available_reports,
                               active_report=active_report,
                               **filter_options)
    except Exception as e:
        return render_template('index.html', error=f"Erro crítico ao carregar a página: {e}", available_reports=get_available_reports())


@main_bp.route('/add-data', methods=['POST'])
def add_data():
    if 'new_data_file' not in request.files or request.files['new_data_file'].filename == '':
        flash('Nenhum arquivo selecionado.', 'error')
        return redirect(url_for('main.index'))
    file = request.files['new_data_file']
    if file and file.filename.endswith('.xlsx'):
        try:
            data_dir = get_data_dir()
            os.makedirs(data_dir, exist_ok=True)
            file_path = os.path.join(data_dir, file.filename)
            file.save(file_path)
            clear_caches()
            flash(f'Relatório "{file.filename}" foi salvo/atualizado com sucesso!', 'success')
            return redirect(url_for('main.index', report=file.filename))
        except Exception as e:
            flash(f'Ocorreu um erro ao salvar o arquivo: {e}', 'error')
    else:
        flash('Formato de arquivo inválido. Por favor, envie um arquivo .xlsx.', 'error')
    return redirect(url_for('main.index'))

@main_bp.route('/clear-data', methods=['POST'])
def clear_data():
    try:
        data_dir = get_data_dir()
        if os.path.exists(data_dir):
            for filename in os.listdir(data_dir):
                if filename.endswith('.xlsx'):
                    os.remove(os.path.join(data_dir, filename))
            clear_caches()
            flash('Todos os relatórios foram removidos com sucesso.', 'success')
        else:
            flash('A pasta de dados não existe.', 'info')
    except Exception as e:
        flash(f'Ocorreu um erro ao limpar os dados: {e}', 'error')
    return redirect(url_for('main.index'))

@main_bp.route('/process-filters', methods=['POST'])
def process_filters():
    active_report = request.form.get('active_report')
    form_data = {
        'report': active_report,
        'ano': request.form.getlist('anos'),
        'produto': request.form.getlist('produtos'),
        'taxa': request.form.getlist('taxas'),
        'emissor': request.form.getlist('emissores'),
        'ir': request.form.getlist('tipos_ir'),
        'report_type': request.form.get('report_type'),
        'liquidez_diaria': request.form.get('liquidez_diaria', 'off'),
    }
    return redirect(url_for('main.show_results', **form_data))

def filter_dataframe(df, args):
    df_filtrado = df.copy()
    anos = [int(a) for a in args.getlist('ano')]
    produtos = args.getlist('produto')
    taxas = args.getlist('taxa')
    emissores = args.getlist('emissor')
    tipos_ir = args.getlist('ir')
    liquidez_diaria = args.get('liquidez_diaria') == 'on'
    if tipos_ir: df_filtrado = df_filtrado[df_filtrado['IR'].isin(tipos_ir)]
    if liquidez_diaria:
        df_filtrado = df_filtrado[df_filtrado['Liquidez_Diaria'] == True]
    else:
        df_filtrado = df_filtrado[df_filtrado['Liquidez_Diaria'] == False]
        if anos: df_filtrado = df_filtrado[df_filtrado['Ano_Vencimento'].isin(anos)]
    if produtos: df_filtrado = df_filtrado[df_filtrado['Tipo_Produto_Base'].isin(produtos)]
    if taxas: df_filtrado = df_filtrado[df_filtrado['Tipo_Taxa'].isin(taxas)]
    if emissores: df_filtrado = df_filtrado[df_filtrado['Emissor'].isin(emissores)]
    return df_filtrado

@main_bp.route('/results', methods=['GET'])
def show_results():
    try:
        active_report = request.args.get('report')
        if not active_report:
            return "<h1>Erro: Relatório não especificado.</h1><p>Por favor, volte à página inicial e selecione um relatório.</p>"
        
        df = get_report_data(active_report)
        if df.empty:
            return f"<h1>Erro ao processar o relatório '{active_report}'.</h1><p>Por favor, verifique o arquivo e tente carregá-lo novamente.</p>"
        
        df_filtrado = filter_dataframe(df, request.args)
        report_type = request.args.get('report_type')
        is_advisor_report = (report_type == 'assessor')
        
        # **CORREÇÃO: Se o relatório for o de Compromissadas, força o modo "cliente" (sem ROA)**
        if active_report and 'compromissada' in active_report.lower():
            is_advisor_report = False

        top_n = 8 if is_advisor_report else 5
        analysis_result = find_best_assets(df_filtrado, top_n=top_n)
        
        liquidez_imediata_assets = analysis_result[analysis_result['Sem_Carencia'] == True]
        liquidez_diaria_assets = analysis_result[(analysis_result['Liquidez_Diaria'] == True) & (analysis_result['Sem_Carencia'] == False)]
        prazo_assets = analysis_result[analysis_result['Liquidez_Diaria'] == False]
        
        return render_template('results.html', 
                               liquidez_imediata_assets=liquidez_imediata_assets,
                               liquidez_diaria_assets=liquidez_diaria_assets,
                               prazo_assets=prazo_assets,
                               is_advisor=is_advisor_report,
                               download_url_params=request.query_string.decode('utf-8'))
    except Exception as e:
        return f"<h1>Ocorreu um erro ao gerar a visualização:</h1><p>{str(e)}</p>", 500

@main_bp.route('/download/<file_format>', methods=['GET'])
def download_file(file_format):
    try:
        active_report = request.args.get('report')
        if not active_report:
            return "Erro: Relatório não especificado.", 400
            
        df = get_report_data(active_report)
        df_filtrado = filter_dataframe(df, request.args)
        report_type = request.args.get('report_type')
        is_advisor_report = (report_type == 'assessor')

        # **CORREÇÃO: Se o relatório for o de Compromissadas, força o modo "cliente" (sem ROA)**
        if active_report and 'compromissada' in active_report.lower():
            is_advisor_report = False

        top_n = 8 if is_advisor_report else 5
        analysis_result = find_best_assets(df_filtrado, top_n=top_n)
        
        if analysis_result.empty: return "Nenhum dado encontrado.", 404
            
        if file_format == 'excel' and is_advisor_report:
            cols_to_keep = ['Produto', 'Emissor_Display', 'Vencimento', 'Taxa_str', 'IR', 'Aplicacao_Minima', 'Roa']
            df_excel = analysis_result[cols_to_keep].copy()
            df_excel.rename(columns={'Taxa_str': 'Taxa', 'Aplicacao_Minima': 'Aplicação Mínima', 'Emissor_Display': 'Emissor'}, inplace=True)
            df_excel['Vencimento'] = df_excel['Vencimento'].dt.strftime('%d/%m/%Y')
            if 'Roa' in df_excel.columns: df_excel['Roa'] = (df_excel['Roa'] * 100).map('{:,.2f}%'.format)
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer: df_excel.to_excel(writer, index=False, sheet_name='Relatorio')
            output.seek(0)
            return Response(output, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": "attachment; filename=relatorio_assessores.xlsx"})
        else:
            base_path = get_base_path()
            logo_path = os.path.join(base_path, 'static', 'logo.png')
            pdf_bytes = create_pdf_report(analysis_result, include_roa=is_advisor_report, logo_path=logo_path)
            filename = "relatorio_assessores.pdf" if is_advisor_report else "relatorio_clientes.pdf"
            response = make_response(pdf_bytes)
            response.headers['Content-Type'] = 'application/pdf'
            response.headers['Content-Disposition'] = f'attachment; filename={filename}'
            return response
    except Exception as e:
        return f"<h1>Ocorreu um erro ao gerar o arquivo:</h1><p>{str(e)}</p>", 500