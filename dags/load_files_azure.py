import os
import sys
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from azure_utils import get_service_client_token_credential,upload_file_azure,create_metadata_file

#sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))



default_args = {
    "owner" : "admin",
    "retries": 0
}

@dag(
    dag_id= "load_csv_to_azure",
    default_args= default_args,
    start_date= datetime(2024,3,23),
    catchup= False,
    schedule_interval= None
)
def load_csv_to_azure(storage_name:str, container_name:str,local_directory_path:str):

    #service_client= get_service_client_token_credential(storage_name)
    script_directory = os.path.dirname(os.path.abspath(__file__))


    @task()
    def upload_transactions_csv(target_path:str,local_directory_path:str, storage_name:str, container_name:str) -> list:
        service_client= get_service_client_token_credential(storage_name)

        current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        
        folder_path = os.path.abspath(os.path.join(script_directory, local_directory_path))

        file_names = [file for file in os.listdir(folder_path) if file.endswith(".csv") and "trans" in file.lower()]

        uploaded_files_path = []
        for file in file_names:
            upload_file_path = upload_file_azure(local_file_name= file, target_path= target_path, current_time= current_time,file_system_name= container_name, service_client= service_client)
            uploaded_files_path.append(upload_file_path)
        return uploaded_files_path
    
    @task()
    def upload_products_csv(target_path:str,local_directory_path:str, storage_name:str, container_name:str) -> list:
        service_client= get_service_client_token_credential(storage_name)

        current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        
        folder_path = os.path.abspath(os.path.join(script_directory, local_directory_path))

        file_names = [file for file in os.listdir(folder_path) if file.endswith(".csv") and "product" in file.lower()]

        uploaded_files_path = []
        for file in file_names:
            upload_file_path = upload_file_azure(local_file_name= file, target_path= target_path, current_time= current_time,file_system_name= container_name, service_client= service_client)
            uploaded_files_path.append(upload_file_path)
        return uploaded_files_path
    
    @task()
    def upload_locations_csv(target_path:str,local_directory_path:str, storage_name:str, container_name:str) -> list:
        service_client= get_service_client_token_credential(storage_name)

        current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        
        folder_path = os.path.abspath(os.path.join(script_directory, local_directory_path))

        file_names = [file for file in os.listdir(folder_path) if file.endswith(".csv") and "location" in file.lower()]

        uploaded_files_path = []
        for file in file_names:
            upload_file_path = upload_file_azure(local_file_name= file, target_path= target_path, current_time= current_time,file_system_name= container_name, service_client= service_client)
            uploaded_files_path.append(upload_file_path)
        return uploaded_files_path
    
    @task()
    def upload_metadata(uploads_files:list, target_path: str, container_name:str):
        current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        flat_uploads = [item for sublist in uploads_files for item in sublist]
        script_directory = os.path.dirname(os.path.abspath(__file__))
        folder_path = os.path.abspath(os.path.join(script_directory, "../data/metadata_loads"))

        file_path, file_name= create_metadata_file(flat_uploads, current_time= current_time, metadata_folder_path= folder_path )

        path = f"{target_path}/{file_name}"

        service_client= get_service_client_token_credential("stgfull1")
        directory_client = service_client.get_file_client(file_system= container_name, file_path= path)

        with open(file=file_path, mode="rb") as data:
            directory_client.upload_data(data, overwrite=True)
    
    transactions_paths = upload_transactions_csv("files/transactions",local_directory_path, storage_name, container_name)
    products_paths = upload_products_csv("files/products",local_directory_path, storage_name, container_name)
    locations_paths = upload_locations_csv("files/locations",local_directory_path, storage_name, container_name)

    uploads_csv = [transactions_paths,products_paths,locations_paths]

    upload_metadata(uploads_csv, "files/metadata",container_name)
test_dag = load_csv_to_azure("stgfull1","airdb2","../data/")
