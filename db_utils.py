import pyodbc
import pandas as pd
import uuid

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
      ,[contador_nome]
      ,[email_login_areacontador]
      ,[generation_date]
      ,[start_date]
      ,[end_date]
      ,[blob_path]
      ,[url_blob_path]
  FROM [dbo].[vw_sl_docs]
  WHERE [cnpj] = ?
  GROUP BY
       [cnpj]
      ,[programa]
      ,[partner_nome]
      ,[contador_nome]
      ,[email_login_areacontador]
      ,[generation_date]
      ,[start_date]
      ,[end_date]
      ,[blob_path]
      ,[url_blob_path]
    '''    
    data = pd.read_sql(query, conn, params=[cnpj])
    conn.close()    
    return data
