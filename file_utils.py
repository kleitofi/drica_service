import streamlit as st
import requests
import os

def save_json_to_file(json_data, file_path):
    try:
        with open(file_path, 'w') as file:
            file.write(json_data)
        print(f"JSON salvo em: {file_path}")
        return True
    except Exception as ex:
        print(f"Erro ao salvar JSON: {ex}")
        return False

def download_file(url):
    """
    Faz o download de um arquivo a partir de uma URL e retorna o conteúdo e o nome do arquivo.

    :param url: URL do arquivo a ser baixado
    :return: conteúdo do arquivo e nome do arquivo extraído da URL
    """
    try:
        # Extrair o nome do arquivo a partir da URL
        output_filename = os.path.basename(url)
        
        # Fazer a requisição GET para o arquivo
        response = requests.get(url)

        # Verificar se a requisição foi bem-sucedida
        if response.status_code == 200:
            return response.content, output_filename
        else:
            st.error(f"Falha no download. Status code: {response.status_code}")
            return None, None
    except Exception as e:
        st.error(f"Ocorreu um erro: {e}")
        return None, None