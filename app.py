import streamlit as st
import pyodbc
import pandas as pd
import uuid
from datetime import datetime

# Função para conectar ao banco de dados
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

# Função para inserir dados na tabela
def insert_data(cnpj, sintegra, sped, start_date, end_date, generation_date):
    guid = str(uuid.uuid4())  # Gerar GUID automaticamente
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO tb_agend_docs (guid, cnpj, sintegra, sped, start_date, end_date, generation_date, pending)
        VALUES (?, ?, ?, ?, ?, ?, ?, 0)
    ''', (guid, cnpj, sintegra, sped, start_date, end_date, generation_date))
    conn.commit()
    cursor.close()
    conn.close()

# Função para buscar dados da tabela
def fetch_data(filter_cnpj=None):
    conn = get_db_connection()
    query = '''
    SELECT cnpj
    , sintegra
    , sped
    , start_date
    , end_date
    , generation_date 
    , 'https://softcomarquivospublicos.blob.core.windows.net/publico/'+[blob_path]+'/'+'Sintegra_'+FORMAT([start_date],'MMyyyy')+'.txt'
    FROM tb_agend_docs
    '''
    if filter_cnpj:
        query += ' WHERE cnpj LIKE ?'
        data = pd.read_sql(query, conn, params=[f'%{filter_cnpj}%'])
    else:
        data = pd.read_sql(query, conn)
    conn.close()
    return data

# Função para formatar as datas
def format_date(date):
    return date.strftime('%d/%m/%Y')

# Título do aplicativo
st.title('Agendamento de Documentos')

# Formulário para inserir dados
with st.form(key='agendamento_form'):
    col1, col2, col3 = st.columns(3)
    with col1:
        cnpj = st.text_input('CNPJ')
        sintegra = st.checkbox('Sintegra')
    with col2:
        generation_date = st.date_input('Generation Date', value=datetime.today(), format='DD/MM/YYYY')
        sped = st.checkbox('SPED')
    with col3:
        date_range = st.date_input("Selecione o intervalo de datas", value=(datetime.today(), datetime.today()), format='DD/MM/YYYY')
        start_date = date_range[0]
        end_date = date_range[1]
    submit_button = st.form_submit_button(label='Inserir')

    if submit_button:
        insert_data(cnpj, sintegra, sped, start_date.strftime('%d/%m/%Y'), end_date.strftime('%d/%m/%Y'), generation_date.strftime('%d/%m/%Y'))
        st.success('Dados inseridos com sucesso!')

# Filtro para buscar agendamentos
filter_cnpj = st.text_input('Filtrar por CNPJ')
filter_button = st.button('Filtrar')

# Exibir dados da tabela
if filter_button or filter_cnpj:
    data = fetch_data(filter_cnpj)
else:
    data = fetch_data()

# Formatando as datas para exibição
data['start_date'] = pd.to_datetime(data['start_date']).dt.strftime('%d/%m/%Y')
data['end_date'] = pd.to_datetime(data['end_date']).dt.strftime('%d/%m/%Y')
data['generation_date'] = pd.to_datetime(data['generation_date']).dt.strftime('%d/%m/%Y')

st.write('Agendamentos Existentes:')
st.dataframe(data)
