from azure.storage.blob import BlobServiceClient, ContentSettings
import os

def send_file(file_path):
    try:
        storage_conn_string = "DefaultEndpointsProtocol=https;AccountName=softcomarquivospublicos;AccountKey=Zp/Sx9HRl39mp6CtSFKSaHd75q3Hj2YliJBGSq7omRIsjybeMh8QbSwN6hKR/Y/M9m/ZhbYv91DJM+sH9m5Bkg==;EndpointSuffix=core.windows.net"
        container_name = "publico"
        past_name = "DocClients"
    
        blob_service_client = BlobServiceClient.from_connection_string(storage_conn_string)
        container_client = blob_service_client.get_container_client(container_name)
        
        file_name = os.path.basename(file_path)
        blob_client = container_client.get_blob_client(f"{past_name}/{file_name}")
        content_settings = ContentSettings(content_type="application/json")
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, blob_type="BlockBlob", content_settings=content_settings, overwrite=True)
        
        print(f"Upload: {file_path}")
        return True
    except Exception as ex:
        print(f"Erro: {ex}")
        return False