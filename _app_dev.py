import streamlit as st
import pyodbc
from datetime import datetime
import pandas as pd
import uuid
from azure.storage.blob import BlobServiceClient, ContentSettings
import os
import webbrowser
from urllib.parse import urlparse

def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False
    
def get_db_connection():
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=201.16.182.210\SQL2017STD,8701;'
        'DATABASE=dbDricaService;'
        'UID=sa_softcom;'
        'PWD=S6t3G@;'
        'MultipleActiveResultSets=True;'
        'Persist Security Info=False;'
    )
    return conn

def fetch_data_as_json():
    conn = get_db_connection()
    query = """
    SELECT 
        x.guid,
        x.cnpj,
        x.sintegra,
        x.start_date,
        x.end_date,
        x.generation_date,
        x.blob_path
    FROM tb_agend_docs AS x
    """
    try:
        data = pd.read_sql(query, conn)
        
        for date_column in ['start_date', 'end_date', 'generation_date']:
            data[date_column] = pd.to_datetime(data[date_column], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S')
        
        data['sintegra'] = data['sintegra'].astype(bool)
        
        json_data = data.to_json(orient='records', indent=4)
        return json_data
    except Exception as ex:
        print(f"Erro: {ex}")
        return None

def save_json_to_file(json_data, file_path):
    try:
        with open(file_path, 'w') as file:
            file.write(json_data)
        print(f"JSON salvo em: {file_path}")
        return True
    except Exception as ex:
        print(f"Erro ao salvar JSON: {ex}")
        return False

def send_file(file_path):
    try:
        storage_conn_string = "DefaultEndpointsProtocol=https;AccountName=softcomarquivospublicos;AccountKey=Zp/Sx9HRl39mp6CtSFKSaHd75q3Hj2YliJBGSq7omRIsjybeMh8QbSwN6hKR/Y/M9m/ZhbYv91DJM+sH9m5Bkg==;EndpointSuffix=core.windows.net"
        container_name = "publico"
        past_name = "DocClients"
    
        blob_service_client = BlobServiceClient.from_connection_string(storage_conn_string)
        container_client = blob_service_client.get_container_client(container_name)
        
        file_name = os.path.basename(file_path)
        blob_client = container_client.get_blob_client(f"{past_name}/{file_name}")
        content_settings = ContentSettings(content_type="application/json")
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, blob_type="BlockBlob", content_settings=content_settings, overwrite=True)
        
        print(f"Upload: {file_path}")
        return True
    except Exception as ex:
        print(f"Erro: {ex}")
        return False

def insert_data(cnpj, sintegra, sped, start_date, end_date, generation_date):
    guid = str(uuid.uuid4())
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO tb_agend_docs (guid, cnpj, sintegra, sped, start_date, end_date, generation_date, pending)
        VALUES (?, ?, ?, ?, ?, ?, ?, 0)
    ''', (guid, cnpj, sintegra, sped, start_date, end_date, generation_date))
    conn.commit()
    cursor.close()
    conn.close()

def fetch_data_client(filter_cnpj=None):
    conn = get_db_connection()
    base_query = '''
SELECT TOP 20 [cnpj]
      ,[nome]
      ,[partner_nome]
      ,[quant_ag]
FROM [vw_sl_clients]
    '''
    
    filter_query = ''
    if filter_cnpj:
        filter_query = " WHERE CONCAT([cnpj],[nome]) LIKE ?"
    
    order = " ORDER BY [quant_ag] DESC"
    
    query = base_query + filter_query + order

    params = [f'%{filter_cnpj}%'] if filter_cnpj else []

    data = pd.read_sql(query, conn, params=params)
    conn.close()
    return data

def fetch_data_detail(cnpj):
    conn = get_db_connection()
    query = '''
SELECT [cnpj]
    ,[programa]
    ,[partner_nome]
    ,[generation_date]
    ,[start_date]
    ,[end_date]
    ,[url_blob_path]
FROM [vw_sl_docs]
WHERE [cnpj] = ?
GROUP BY [cnpj]
    ,[programa]
    ,[partner_nome]
    ,[generation_date]
    ,[start_date]
    ,[end_date]
    ,[url_blob_path]
    '''    
    data = pd.read_sql(query, conn, params=[cnpj])
    conn.close()    
    return data

def generate_dynamic_table(data):
    st.write("### Lista de Dados com Ações")
    st.markdown(
        """
        <style>
        .stButton button {
            width: 100%;
            height: 100%;
        }
        .first-column {
            flex: 2;
        }
        .other-columns {
            flex: 1;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    for i, row in data.iterrows():
        cols = st.columns([7, 1, 1])
        cols[0].write(f"{row['cnpj']}-{row['nome']}")
        cols[1].write('Qtd: ' + str(row["quant_ag"]))
        if cols[2].button("Ação", key=f"button_{i}"):
            st.session_state.selected_cnpj = row['cnpj']
            detailed_data = fetch_data_detail(row['cnpj'])
            st.session_state.detailed_data = detailed_data
            st.session_state.show_details = True
            st.experimental_rerun()

def generate_detailed_view(data):
    for i, row in data.iterrows():
        st.write("#####  --------- Detalhes do Cliente ---------")
        st.write(f"**Programa:** {row['programa']}")
        st.write(f"**Nome do Parceiro:** {row['partner_nome']}")
        st.write(f"**Data de Geração:** {row['generation_date']}")
        st.write(f"**Data de Início:** {row['start_date']}")
        st.write(f"**Data de Término:** {row['end_date']}")
        url_blob_path = row['url_blob_path']
        
        with st.expander("Ações"):
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("Baixar Arquivo", key=f"download_{i}"):                
                    st.write(f"Baixando arquivo: {url_blob_path}")
            with col2:
                if st.button("Abrir Arquivo", key=f"open_{i}"):
                    if is_valid_url(url_blob_path):
                        webbrowser.open_new_tab(url_blob_path)
                    else:
                        st.error("URL inválida!")
            with col3:
                if st.button("Enviar por Email", key=f"email_{i}"):
                    st.write(f"Enviando email para: {url_blob_path}")

st.title('Agendamento de Documentos')

if st.button('Sync Dados'):
    json_result = fetch_data_as_json()
    if json_result:
        file_path = "clients_config.json"
        if save_json_to_file(json_result, file_path):
            send_file(file_path)

with st.expander("Inserir Dados", expanded=True):
    with st.form(key='agendamento_form'):
        st.subheader("Inserir Novo Documento")
        col1, col2, col3 = st.columns(3)
        with col1:
            cnpj = st.text_input('CNPJ')
            sintegra = st.checkbox('Sintegra')
        with col2:
            generation_date = st.date_input('Data de Geração', value=datetime.today(), format='DD/MM/YYYY')
            sped = st.checkbox('SPED')
        with col3:
            date_range = st.date_input("Intervalo de Datas", value=(datetime.today(), datetime.today()), format='DD/MM/YYYY')
            start_date, end_date = date_range[0], date_range[1]
        if st.form_submit_button('Inserir'):
            insert_data(cnpj, sintegra, sped, start_date, end_date, generation_date)
            st.success('Dados inseridos com sucesso!')

if 'show_details' not in st.session_state:
    st.session_state.show_details = False

if st.session_state.show_details:
    if 'detailed_data' in st.session_state:
        generate_detailed_view(st.session_state.detailed_data)
    if st.button('Voltar'):
        st.session_state.show_details = False
        st.experimental_rerun()
else:
    with st.expander("Pesquisar e Lista de Clientes", expanded=True):
        filter_cnpj = st.text_input('Filtrar por CNPJ ou Nome')
        if st.button('Filtrar') or filter_cnpj:
            data = fetch_data_client(filter_cnpj)
        else:
            data = fetch_data_client()
        generate_dynamic_table(data)
