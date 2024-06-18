import streamlit as st
import webbrowser
from urllib.parse import urlparse
from db_utils import *
from file_utils import *
import pandas as pd
import base64

def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

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

def format_date(date_str):
    return pd.to_datetime(date_str).strftime('%d-%m-%Y')

def generate_detailed_view(data):
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
        formatted_generation_date = format_date(row['generation_date'])
        expander_label = f"Detalhes do Cliente (Geração: {formatted_generation_date})"
        
        with st.expander(expander_label):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**CNPJ:** {row['cnpj']}")
                st.write(f"**Programa:** {row['programa']}")
                st.write(f"**Parceiro Nome:** {row['partner_nome']}")
                st.write(f"**Contador Nome:** {row['contador_nome']}")
                st.write(f"**Contador Email:** {row['email_login_areacontador']}")
            with col2:
                st.write(f"**Data Geração:** {formatted_generation_date}")
                st.write(f"**Data Início:** {format_date(row['start_date'])}")
                st.write(f"**Data Término:** {format_date(row['end_date'])}")
            
            url_blob_path = row['url_blob_path']
            
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("Baixar Arquivo", key=f"download_{i}"): 
                    conteudo_arquivo, nome_arquivo = download_file(url_blob_path)
                    
                    if conteudo_arquivo and nome_arquivo:
                        b64 = base64.b64encode(conteudo_arquivo).decode()
                        href = f'<a href="data:application/octet-stream;base64,{b64}" download="{nome_arquivo}">Clique aqui para baixar</a>'
                        st.markdown(href, unsafe_allow_html=True)
                    else:
                        st.error("Não foi possível baixar o arquivo.")

            with col2:
                if st.button("Abrir Arquivo", key=f"open_{i}"):
                    if is_valid_url(url_blob_path):
                        webbrowser.open_new_tab(url_blob_path)
                    else:
                        st.error("URL inválida!")

            with col3:
                if st.button("Enviar por Email", key=f"email_{i}"):
                    st.write(f"Enviando email para: {url_blob_path}")