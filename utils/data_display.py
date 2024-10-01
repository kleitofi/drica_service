import streamlit as st
import streamlit_antd_components as sac
import webbrowser
from urllib.parse import urlparse
from utils.db_utils import *
from utils.file_utils import *
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
        drica = row["drica"]
        emoji_bot = "🤖" if drica else "📜"
        
        quant_ag = row["quant_ag"]
        orders_ok = row["orders_ok"]
        stat = f'({quant_ag} - {orders_ok})'
        
        emoji_stat = "⬜" if quant_ag == 0 else "✅" if quant_ag == orders_ok else "🟧" 
        cols = st.columns([7, 2, 1])
        cols[0].write(f"{row['cnpj']}{emoji_bot}{row['nome']}")
        cols[1].write(f'Stat: {stat} {emoji_stat}')
        if cols[2].button("...", key=f"button_{i}"):
            st.session_state.selected_cnpj = row['cnpj']
            detailed_data = fetch_data_detail(row['cnpj'])
            st.session_state.detailed_data = detailed_data
            st.session_state.show_details = True
            st.rerun()

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
        emoji = "✅" if row['strSummaryOrders'] else "⚪"
        expander_label = f"Detalhes do Cliente (Geração: {formatted_generation_date}) {emoji}"
        
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
            
            col1, col2, col3 = st.columns(3)  # Adjusting to 3 columns
            
            with col1:
                if st.button("Enviar por Email", key=f"email_{i}", disabled=True):
                    st.write(f"Enviando email para: {url_blob_path}")
            
            with col2:
                if st.button("Baixar Arquivo", key=f"download_{i}"): 
                    conteudo_arquivo, nome_arquivo = download_file(url_blob_path)
                    
                    if conteudo_arquivo and nome_arquivo:
                        b64 = base64.b64encode(conteudo_arquivo).decode()
                        href = f'<a href="data:application/octet-stream;base64,{b64}" download="{nome_arquivo}">Clique aqui para baixar</a>'
                        st.markdown(href, unsafe_allow_html=True)
                    else:
                        st.error("Não foi possível baixar o arquivo.")
                
            with col3:
                if st.button("Abrir Detalhes", key=f"details_{i}"):
                    show_details = True
                else:
                    show_details = False
            
            if show_details:
                st.write("Detalhes do Pedido:")
                st.code(row['strSummaryOrders'], language='text')  # Mostrando detalhes abaixo dos botões
