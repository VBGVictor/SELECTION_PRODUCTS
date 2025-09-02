from flask import Blueprint, render_template, make_response, request, redirect, url_for, Response, flash
from .data_processor import process_data
from .analysis import find_best_assets
from .pdf_generator import create_pdf_report
import os
import io
import pandas as pd

main_bp = Blueprint('main', __name__)

def get_base_path():
    return os.path.dirname(os.path.abspath(__file__))

# Cache simples para performance
_cached_data = None

def get_project_root():
    return os.path.dirname(get_base_path())

def get_data_path():
    return os.path.join(get_project_root(), 'data', 'credito bancario.xlsx')

def clear_data_cache():
    """Limpa o cache de dados para forçar a releitura do arquivo."""
    global _cached_data
    _cached_data = None
    
def get_processed_data():
    global _cached_data
    if _cached_data is None:
        file_path = get_data_path()
        _cached_data = process_data(file_path)
    return _cached_data

@main_bp.route('/', methods=['GET'])
def index():
    """Exibe o dashboard de filtros."""
    try:
        df = get_processed_data()
        if df.empty:
            return render_template('index.html', error="Nenhum dado processável foi encontrado no arquivo Excel.")

        anos = sorted(df[~df['Liquidez_Diaria']]['Ano_Vencimento'].unique())
        tipos_produto = sorted(df['Tipo_Produto_Base'].unique())
        tipos_taxa = sorted(df['Tipo_Taxa'].unique())
        emissores = sorted(df[df['Emissor'] != 'N/A']['Emissor'].unique())
        
        return render_template('index.html', anos=anos, tipos_produto=tipos_produto, tipos_taxa=tipos_taxa, emissores=emissores)
    except Exception as e:
        return render_template('index.html', error=f"Erro crítico ao carregar a página: {e}")

@main_bp.route('/update-data', methods=['POST'])
def update_data():
    """Recebe um novo arquivo de dados, substitui o antigo e limpa o cache."""
    if 'new_data_file' not in request.files:
        flash('Nenhum arquivo selecionado.', 'error')
        return redirect(url_for('main.index'))

    file = request.files['new_data_file']

    if file.filename == '':
        flash('Nenhum arquivo selecionado.', 'error')
        return redirect(url_for('main.index'))

    if file and file.filename.endswith('.xlsx'):
        try:
            file_path = get_data_path()
            file.save(file_path)
            clear_data_cache()  # Limpa o cache para que os novos dados sejam lidos
            flash('Dados atualizados com sucesso!', 'success')
        except Exception as e:
            flash(f'Ocorreu um erro ao salvar o arquivo: {e}', 'error')
    else:
        flash('Formato de arquivo inválido. Por favor, envie um arquivo .xlsx.', 'error')

    return redirect(url_for('main.index'))


@main_bp.route('/process-filters', methods=['POST'])
def process_filters():
    """Recebe os filtros e redireciona para a página de resultados."""
    form_data = {
        'ano': request.form.getlist('anos'),
        'produto': request.form.getlist('produtos'),
        'taxa': request.form.getlist('taxas'),
        'emissor': request.form.getlist('emissores'),
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
    liquidez_diaria = args.get('liquidez_diaria') == 'on'
    
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
    """Exibe a página de resultados (pode ser atualizada sem avisos)."""
    try:
        df = get_processed_data()
        df_filtrado = filter_dataframe(df, request.args)
        
        report_type = request.args.get('report_type')
        is_advisor_report = (report_type == 'assessor')
        top_n = 8 if is_advisor_report else 5
        analysis_result = find_best_assets(df_filtrado, top_n=top_n)

        # A lógica de filtragem foi movida para cá
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
        df = get_processed_data()
        df_filtrado = filter_dataframe(df, request.args)
        
        report_type = request.args.get('report_type')
        is_advisor_report = (report_type == 'assessor')
        top_n = 8 if is_advisor_report else 5
        analysis_result = find_best_assets(df_filtrado, top_n=top_n)

        if analysis_result.empty: return "Nenhum dado encontrado.", 404

        if file_format == 'excel' and is_advisor_report:
            cols_to_keep = ['Produto', 'Emissor_Display', 'Vencimento', 'Taxa_str', 'Aplicacao_Minima', 'Roa']
            df_excel = analysis_result[cols_to_keep].copy()
            df_excel.rename(columns={'Taxa_str': 'Taxa', 'Aplicacao_Minima': 'Aplicação Mínima', 'Emissor_Display': 'Emissor'}, inplace=True)
            df_excel['Vencimento'] = df_excel['Vencimento'].dt.strftime('%d/%m/%Y')
            if 'Roa' in df_excel.columns: df_excel['Roa'] = (df_excel['Roa'] * 100).map('{:,.2f}%'.format)
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer: df_excel.to_excel(writer, index=False, sheet_name='Relatorio')
            output.seek(0)
            return Response(output, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": "attachment; filename=relatorio_assessores.xlsx"})
        else: # PDF
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