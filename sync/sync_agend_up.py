import pyodbc
from azure.data.tables import TableServiceClient
from azure.core.credentials import AzureSasCredential

# Configuração da conexão com o Azure Table Storage
account_url = 'https://dricastg.table.core.windows.net/'
sas_token = 'sv=2017-04-17&si=All&sig=YEDfKhQLR%2BcfBfiRFzDwYkl17FRQF3uwBHFToFF8tm0%3D&tn=ClientsDocFiscal'
table_name = 'ClientsDocFiscal'

# Criação do cliente da tabela do Azure
credential = AzureSasCredential(sas_token)
service_client = TableServiceClient(endpoint=account_url, credential=credential)
table_client = service_client.get_table_client(table_name)

# Função para conectar ao banco de dados SQL Server
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

# Função para obter os pares de PartitionKey (cnpj) e RowKey (guid) da consulta SQL
def get_partition_and_row_keys(cursor):
    query = "SELECT guid, cnpj FROM [vw_sync_agend_ok]"
    cursor.execute(query)
    return cursor.fetchall()

# Função para atualizar o campo 'pending' no banco de dados tb_agend_docs com base em guid e cnpj
def update_pending_in_db(cursor, row_key, partition_key):
    try:
        # Atualiza o campo pending para true na tabela tb_agend_docs usando guid e cnpj
        update_query = """
        UPDATE tb_agend_docs
        SET pending = 1
        WHERE guid = ? AND cnpj = ?
        """
        cursor.execute(update_query, row_key, partition_key)
        cursor.commit()
        print(f"Atualizado no banco de dados - RowKey (guid): {row_key}, PartitionKey (cnpj): {partition_key}")
    except Exception as e:
        print(f"Erro ao atualizar no banco de dados: {e}")

# Função para atualizar a coluna 'pending' para True no Azure e no banco de dados
def update_pending_to_true(keys_to_update, conn):
    try:
        cursor = conn.cursor()
        
        # Itera sobre os pares (guid, cnpj) retornados pela consulta
        for row in keys_to_update:
            row_key = row.guid
            partition_key = row.cnpj

            # Busca a entidade específica com PartitionKey e RowKey no Azure Table Storage
            entity = table_client.get_entity(partition_key=partition_key, row_key=row_key)
            if entity:
                # Atualiza o campo 'pending' para True no Azure Table Storage
                entity['pending'] = True
                table_client.upsert_entity(entity)  # Atualiza ou insere no Azure Table Storage
                print(f"Atualizado no Azure - PartitionKey: {partition_key}, RowKey: {row_key}")

                # Atualiza o campo 'pending' para True no banco de dados SQL Server
                update_pending_in_db(cursor, row_key, partition_key)

        # Confirma as mudanças no banco de dados        
        print("Atualização concluída para as entidades especificadas no Azure e no banco de dados.")

    except Exception as e:
        print(f"Erro ao atualizar entidades: {e}")

# Função principal para orquestrar a atualização
def sync_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Obter os pares de PartitionKey (cnpj) e RowKey (guid)
        keys_to_update = get_partition_and_row_keys(cursor)
        print(f"Total de registros a atualizar: {len(keys_to_update)}")

        # Atualizar as entidades no Azure Table Storage e no banco de dados SQL Server
        update_pending_to_true(keys_to_update, conn)

    except Exception as e:
        print(f"Erro ao sincronizar dados: {e}")

    finally:
        cursor.close()
        conn.close()

# Executar a função de sincronização
sync_data()