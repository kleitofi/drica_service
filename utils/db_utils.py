import pyodbc
import pandas as pd
import uuid

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

def fetch_data_sl_clients_performance():
    query = """
SELECT 
    format(y.start_date, 'MM') as 'month',
    COUNT(DISTINCT x.cnpj) AS 'total_clients',
    ISNULL(SUM(CASE WHEN z.strSummaryOrders IS NOT NULL THEN 1 ELSE 0 END), 0) AS 'orders_ok',
	IIF(x.partner_nome LIKE '', 'SOFTCOM', x.partner_nome ) AS 'partner_nome'
FROM 
    vw_agenda_clients AS x
LEFT JOIN 
    vw_agend_docs_insigth AS y 
    ON (CONVERT(NVARCHAR, x.cnpj) COLLATE SQL_Latin1_General_CP1_CI_AI = CONVERT(NVARCHAR, y.cnpj) COLLATE SQL_Latin1_General_CP1_CI_AI)
LEFT JOIN 
    vw_agend_docs_insigth AS z 
    ON (y.guid = z.guid)
WHERE 
    x.id > 0 
    AND y.cnpj IS NOT NULL
	--and x.partner_nome like 'MG - BELO HORIZONTE'
GROUP BY 
    format(y.start_date, 'MM'),x.partner_nome 
ORDER BY 
    4;
    """
    conn = get_db_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def fetch_data_as_json():
    # Simulando a função fetch_data_as_json
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
        print('GET ALL')
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

def fetch_data_client(filter_cnpj=None, selected_partner=None):
    conn = get_db_connection()
    base_query = '''
    SELECT TOP 20 *
    FROM [vw_sl_clients]
    '''
    
    filter_conditions = []
    params = []

    # Verifica se o filtro por CNPJ foi fornecido
    if filter_cnpj:
        filter_conditions.append("CONCAT([cnpj], [nome]) LIKE ?")
        params.append(f'%{filter_cnpj}%')
    
    # Verifica se o filtro por partner foi fornecido (exceto se for 'Todos')
    if selected_partner:
        filter_conditions.append("[partner_nome] = ?")
        params.append(selected_partner)

    # Monta a cláusula WHERE se houver filtros
    filter_query = ''
    if filter_conditions:
        filter_query = " WHERE " + " AND ".join(filter_conditions)
    
    order = " ORDER BY [orders_ok] DESC"
    
    # Monta a query final
    query = base_query + filter_query + order

    # Executa a consulta com os parâmetros
    data = pd.read_sql(query, conn, params=params)
    conn.close()
    return data

def get_partners_list():
    conn = get_db_connection()
    
    # Query para buscar o campo partner_nome da view
    query = '''
    SELECT DISTINCT [partner_nome]
    FROM [vw_sl_clients]
    WHERE [partner_nome] IS NOT NULL
    ORDER BY [partner_nome] ASC
    '''
    
    # Executa a query e retorna os resultados
    partners_df = pd.read_sql(query, conn)
    conn.close()
    
    # Converte a coluna 'partner_nome' em uma lista
    partners_list = partners_df['partner_nome'].tolist()
    
    return partners_list

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
      ,[strSummaryOrders]
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
      ,[strSummaryOrders]
    '''    
    data = pd.read_sql(query, conn, params=[cnpj])
    conn.close()    
    return data
