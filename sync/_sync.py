import pandas as pd
import json
from azure.data.tables import TableServiceClient, TableEntity
from azure.core.credentials import AzureSasCredential
import pyodbc

# Defina as variáveis de configuração
account_url = 'https://dricastg.table.core.windows.net/'
sas_token = 'sv=2017-04-17&si=All&sig=YEDfKhQLR%2BcfBfiRFzDwYkl17FRQF3uwBHFToFF8tm0%3D&tn=ClientsDocFiscal'
table_name = 'ClientsDocFiscal'

# Crie o cliente de serviço de tabela
credential = AzureSasCredential(sas_token)
service_client = TableServiceClient(endpoint=account_url, credential=credential)

# Referência à tabela
table_client = service_client.get_table_client(table_name)

def get_db_connection():
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=177.69.189.17\SQL2017STD,8701;'
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
    SELECT *
    FROM vw_agend_docs_insigth AS x
    """
    try:
        data = pd.read_sql(query, conn)
        
        for date_column in ['start_date', 'end_date', 'generation_date']:
            data[date_column] = pd.to_datetime(data[date_column], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S')
        
        data['sintegra'] = data['sintegra'].astype(bool)
        data['pending'] = data['pending'].astype(bool)
        
        json_data = data.to_json(orient='records', indent=4)
        print('SELECT ALL')
        return json_data
    except Exception as ex:
        print(f"Erro: {ex}")
        return None

def get_existing_entities():
    entities = table_client.list_entities(results_per_page=1000)
    existing_data = []
    for page in entities.by_page():
        for entity in page:
            existing_data.append({
                'cnpj': entity['PartitionKey'],
                'guid': entity['RowKey'],
                'sintegra': entity['sintegra'],
                'start_date': entity['start_date'],
                'end_date': entity['end_date'],
                'generation_date': entity['generation_date'],
                'pending': entity['pending'],
                'blob_path': entity['blob_path']
            })
    return existing_data

def insert_or_update_data(json_data, allow_update=True, allow_insert=True):
    new_entities = json.loads(json_data)
    existing_entities = get_existing_entities()
    existing_dict = {(e['cnpj'], e['guid']): e for e in existing_entities}
    
    for entity in new_entities:
        if not entity['cnpj'] or not entity['guid']:
            print(f"Dados faltando para {entity['guid']} {entity['cnpj']}. Ignorando...")
            continue
        
        # Remover caracteres especiais do CNPJ para o PartitionKey
        partition_key = entity['cnpj'].replace('.', '').replace('/', '').replace('-', '')
        key = (partition_key, entity['guid'])
        
        try:
            if key in existing_dict:
                existing_entity = existing_dict[key]
                if allow_update and entity != existing_entity:
                    table_entity = TableEntity(
                        PartitionKey=entity['cnpj'].replace('.', '').replace('/', '').replace('-', ''),
                        RowKey=entity['guid'],
                        sintegra=entity['sintegra'],
                        start_date=entity['start_date'],
                        end_date=entity['end_date'],
                        generation_date=entity['generation_date'],
                        pending=entity['pending'],
                        blob_path=entity['blob_path']
                    )
                    table_client.update_entity(entity=table_entity, mode='replace')
                    print(f"UPDATE {entity['guid']}")
            else:
                if allow_insert:
                    table_entity = TableEntity(
                        PartitionKey=entity['cnpj'].replace('.', '').replace('/', '').replace('-', ''),
                        RowKey=entity['guid'],
                        sintegra=entity['sintegra'],
                        start_date=entity['start_date'],
                        end_date=entity['end_date'],
                        generation_date=entity['generation_date'],
                        pending=entity['pending'],
                        blob_path=entity['blob_path']
                    )
                    table_client.create_entity(entity=table_entity)
                    print(f"INSERT {entity['guid']}")
        except Exception as ex:
            print(f"Erro ao processar {entity['guid']}: {ex}")
            continue

    print("SYNC END")

# Suponha que fetch_data_as_json já retorna dados JSON válidos
json_data = fetch_data_as_json()

if json_data:
    insert_or_update_data(json_data, allow_update=True, allow_insert=False)
