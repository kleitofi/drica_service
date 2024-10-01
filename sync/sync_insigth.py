import pyodbc
from azure.data.tables import TableServiceClient
from azure.core.credentials import AzureSasCredential
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Configuração da conexão com o Azure Table Storage
account_url = 'https://dricastg.table.core.windows.net/'
sas_token = 'sv=2017-04-17&si=All&sig=YrlxUo8N4J5MSJXcTzOdVGVngd42Lpbk4QT0o39cmkI%3D&tn=InsigthDrica'
table_name = 'InsigthDrica'

# Criação do cliente de tabela do Azure
credential = AzureSasCredential(sas_token)
service_client = TableServiceClient(endpoint=account_url, credential=credential)
table_client = service_client.get_table_client(table_name)

# Variável de controle de sincronização
sync_mode = 'ultima_data'  # Opções: 'tudo', 'data_especifica', 'ultima_data'
data_especifica = datetime(2024, 8, 1)  # Definir a data específica se o modo for 'data_especifica'

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

# Função para carregar todas as combinações PartitionKey e RowKey existentes no banco de dados
def load_existing_keys(cursor):
    query = "SELECT PartitionKey, RowKey FROM tb_insigth"
    cursor.execute(query)
    existing_keys = set()
    for row in cursor.fetchall():
        existing_keys.add((row.PartitionKey, row.RowKey))
    return existing_keys

# Função para obter a lista de cnpj e guid da tabela vw_data_sync
def get_cnpj_and_guid(cursor):
    query = "SELECT cnpj, guid FROM [vw_sync_data_insigth]"
    cursor.execute(query)
    return cursor.fetchall()

# Função que executa a inserção de um lote de dados no banco de dados
def process_batch(batch_pairs, conn, existing_keys, log_file):
    # Criar uma nova conexão para cada thread
    conn = get_db_connection()
    cursor = conn.cursor()
    
    insert_query = """
    INSERT INTO tb_insigth 
    (PartitionKey, RowKey, guidAgent, cnpj, company, start_date, end_date, strSummaryOrders, Nfce_Db, NfeOut_Db, NfeIn_Db, Nfce_Nuvem, Nfe_Nuvem, NfceNaoLocalizada_Nuvem, Timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    partitioned_pairs = partition_data(batch_pairs)

    for cnpj, guids in partitioned_pairs.items():
        query_filter = build_query_filter(cnpj, guids)
        entities = list(table_client.query_entities(query_filter, results_per_page=1000))

        # Processar cada entidade retornada
        for entity in entities:
            partition_key = entity['PartitionKey']
            row_key = entity['RowKey']
            timestamp = entity._metadata['timestamp']

            # Verificar se a entidade já existe no banco ou na memória
            if (partition_key, row_key) in existing_keys:
                log_file.write(f"Registro existente - PartitionKey: {partition_key}, RowKey: {row_key}\n")
            else:
                try:
                    cursor.execute(insert_query, (
                        partition_key,
                        row_key,
                        entity['guidAgent'],
                        entity['cnpj'],
                        entity['company'],
                        entity['start_date'],
                        entity['end_date'],
                        entity['strSummaryOrders'],
                        entity['Nfce_Db'],
                        entity['NfeOut_Db'],
                        entity['NfeIn_Db'],
                        entity['Nfce_Nuvem'],
                        entity['Nfe_Nuvem'],
                        entity['NfceNaoLocalizada_Nuvem'],
                        timestamp
                    ))
                    print(f"Novo registro inserido - PartitionKey: {partition_key}, RowKey: {row_key}\n")
                    log_file.write(f"Novo registro inserido - PartitionKey: {partition_key}, RowKey: {row_key}\n")
                    existing_keys.add((partition_key, row_key))  # Adicionar à memória
                except Exception as ex:
                    print(f"Erro ao inserir {row_key}: {ex}")

    conn.commit()

# Função para particionar os pares de cnpj e guid
def partition_data(batch_pairs):
    partitioned_pairs = {}
    for cnpj, guid in batch_pairs:
        if cnpj not in partitioned_pairs:
            partitioned_pairs[cnpj] = []
        partitioned_pairs[cnpj].append(guid)
    return partitioned_pairs

# Função para construir o filtro de consulta baseado no modo de sincronização
def build_query_filter(cnpj, guids):
    guid_filter = " or ".join([
        f"guidAgent eq '{guid}'" for guid in guids
    ])

    if sync_mode == 'tudo':
        query_filter = f"PartitionKey eq '{cnpj}'"
    elif sync_mode == 'data_especifica':
        query_filter = f"PartitionKey eq '{cnpj}' and Timestamp gt datetime'{data_especifica.isoformat()}'"
    else:  # 'ultima_data' ou outro modo
        query_filter = f"PartitionKey eq '{cnpj}' and ({guid_filter})"
    
    return query_filter

# Função para registrar o início da sincronização e abrir o log
def initialize_sync(cursor):
    existing_keys = load_existing_keys(cursor)
    log_file = open("log.txt", "w")
    log_file.write("Log de sincronização de registros:\n")
    start_time = datetime.now()
    print(f"Início da sincronização: {start_time}")
    return existing_keys, log_file

# Função para finalizar a sincronização, fechando conexões e logs
def finalize_sync(cursor, conn, log_file):
    cursor.close()
    conn.close()
    log_file.close()
    end_time = datetime.now()
    print(f"Sincronização concluída: {end_time}")

# Função principal para orquestrar a sincronização
def fetch_and_insert_new_data(batch_size=100, max_workers=5):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Inicializar a sincronização e carregar as chaves existentes
    existing_keys, log_file = initialize_sync(cursor)

    # Obter a lista de cnpj e guid da tabela vw_data_sync
    cnpj_guid_list = get_cnpj_and_guid(cursor)
    total_pairs = len(cnpj_guid_list)

    # Processamento paralelo dos lotes
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for batch_start in range(0, total_pairs, batch_size):
            batch_end = min(batch_start + batch_size, total_pairs)
            batch_pairs = cnpj_guid_list[batch_start:batch_end]
            executor.submit(process_batch, batch_pairs, conn, existing_keys, log_file)

    # Finalizar a sincronização
    finalize_sync(cursor, conn, log_file)

# Função para sincronizar os dados
def sync_data():
    fetch_and_insert_new_data()
    print("Sincronização de dados concluída.")

# Executar a sincronização
sync_data()
