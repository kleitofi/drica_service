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
        
        key = (entity['cnpj'], entity['guid'])
        try:
            if key in existing_dict:
                existing_entity = existing_dict[key]
                if allow_update and entity != existing_entity:
                    table_entity = TableEntity(
                        PartitionKey=entity['cnpj'],
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
                        PartitionKey=entity['cnpj'],
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

def insert_data_into_table(json_data):
    entities = json.loads(json_data)
    
    for entity in entities:
        try:
            table_entity = TableEntity(
                PartitionKey=entity['cnpj'],
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
            print(f"INSERT Erro {entity['guid']}: {ex}")

    print("INSERT END")
    
# Função para excluir todos os registros
def delete_all_items():
    entities = table_client.list_entities()
    
    for entity in entities:
        table_client.delete_entity(partition_key=entity['PartitionKey'], row_key=entity['RowKey'])
        #print(f'DEL {entity['RowKey']}')

def sync_data_azure_table(json_data):
    # Execute a função de exclusão
    #delete_all_items()

    if json_data:
        insert_or_update_data(json_data, allow_update=False, allow_insert=True)
