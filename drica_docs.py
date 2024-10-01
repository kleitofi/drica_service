import streamlit as st
from datetime import datetime
from utils.db_utils import *
from utils.file_utils import * 
from utils.azure_utils import *
from utils.data_display import *

# Configura o layout da página para Wide Mode
st.set_page_config(layout="centered")

st.title('Agendamento de Documentos')

if st.button('Sync Dados'):
    json_result = fetch_data_as_json()
    if json_result:
        sync_data_azure_table(json_result)

with st.expander("Inserir Dados", expanded=False):
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
        st.rerun()
else:
    with st.expander("Pesquisar e Lista de Clientes", expanded=True):
        filter_cnpj = st.text_input('Filtrar por CNPJ')

        # Obtém a lista de partners e adiciona a opção 'Todos'
        partners = get_partners_list()  
        partners.insert(0, 'Todos')  # Adiciona a opção "Todos" no início da lista

        # Combo box com a lista de partners
        selected_partner = st.selectbox('Selecionar Partner', partners)

        # Verifica se "Todos" foi selecionado e não aplica o filtro de partner
        partner_filter = None if selected_partner == 'Todos' else selected_partner

        # Realiza a filtragem com base no CNPJ e no partner selecionado (ou None se "Todos" for selecionado)
        if st.button('Filtrar') or filter_cnpj or partner_filter:
            data = fetch_data_client(filter_cnpj, partner_filter)
        else:
            data = fetch_data_client()

        generate_dynamic_table(data)
