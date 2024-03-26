import os
import sys
from typing import Tuple
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def get_service_client_token_credential(account_name) -> DataLakeServiceClient:
    account_url = f"https://{account_name}.dfs.core.windows.net"
    token_credential = DefaultAzureCredential()

    service_client = DataLakeServiceClient(account_url, credential=token_credential)

    return service_client

def upload_file_azure(local_file_name:str, target_path:str, current_time:str, file_system_name:str, service_client:DataLakeServiceClient):
    azure_file_name = f"{local_file_name.split('.')[0]}_{current_time}.csv"
    path = f"{target_path}/{azure_file_name}"

    directory_client = service_client.get_file_client(file_system= file_system_name, file_path= path)

    file_path = r"data/{}".format(local_file_name)

    absolute_path = os.path.abspath(file_path)
    print(f"uploading file {azure_file_name} ....")
    with open(file=absolute_path, mode="rb") as data:
        directory_client.upload_data(data, overwrite=True)
    return path

def create_metadata_file(loads:list, current_time:str, metadata_folder_path: str) -> Tuple[str, str]:
    os.makedirs(metadata_folder_path, exist_ok=True)
    metadata_name = f"loads_{current_time}.txt"
    metadata_loads_file_path = os.path.join(metadata_folder_path, metadata_name)

    file_paths_string = "\n".join(loads)

    with open(metadata_loads_file_path, "w") as metadata_loads_file:
        metadata_loads_file.write(file_paths_string)
    return metadata_loads_file_path,metadata_name

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
def test():
    @task()
    def upload_transactions_csv(storage_name:str ,local_directory_path:str , target_path:str) -> list:
        service_client= get_service_client_token_credential(storage_name)
        #system_client= service_client.get_file_system_client(file_system=container)

        current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        script_directory = os.path.dirname(os.path.abspath(__file__))
        folder_path = os.path.abspath(os.path.join(script_directory, local_directory_path))
        #folder_path = os.path.abspath(r"{}".format(local_directory_path))

        file_names = [file for file in os.listdir(folder_path) if file.endswith(".csv") and "trans" in file.lower()]

        uploaded_files_path = []
        for file in file_names:
            print(file)
            print("============================================================")
            upload_file_path = upload_file_azure(local_file_name= file, target_path= target_path, current_time= current_time,file_system_name= "airdb2", service_client= service_client)
            uploaded_files_path.append(upload_file_path)
        return uploaded_files_path
    
    @task()
    def upload_products_csv(storage_name:str ,local_directory_path:str , target_path:str) -> list:
        service_client= get_service_client_token_credential(storage_name)

        current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        script_directory = os.path.dirname(os.path.abspath(__file__))
        folder_path = os.path.abspath(os.path.join(script_directory, local_directory_path))
        #folder_path = os.path.abspath(r"{}".format(local_directory_path))

        file_names = [file for file in os.listdir(folder_path) if file.endswith(".csv") and "product" in file.lower()]

        uploaded_files_path = []
        for file in file_names:
            upload_file_path = upload_file_azure(local_file_name= file, target_path= target_path, current_time= current_time,file_system_name= "airdb2", service_client= service_client)
            uploaded_files_path.append(upload_file_path)
        return uploaded_files_path
    
    @task()
    def upload_locations_csv(storage_name:str ,local_directory_path:str , target_path:str) -> list:
        service_client= get_service_client_token_credential(storage_name)

        current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        script_directory = os.path.dirname(os.path.abspath(__file__))
        folder_path = os.path.abspath(os.path.join(script_directory, local_directory_path))
        #folder_path = os.path.abspath(r"{}".format(local_directory_path))
        current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        file_names = [file for file in os.listdir(folder_path) if file.endswith(".csv") and "location" in file.lower()]

        uploaded_files_path = []
        for file in file_names:
            upload_file_path = upload_file_azure(local_file_name= file, target_path= target_path, current_time= current_time,file_system_name= "airdb2", service_client= service_client)
            uploaded_files_path.append(upload_file_path)
        return uploaded_files_path
    
    @task()
    def upload_metadata(uploads_files:list, storage_name:str, target_path: str, container:str):
        current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        # for i in uploads_files:
        #     print("==================================")
        #     print(i)
        flat_uploads = [item for sublist in uploads_files for item in sublist]
        script_directory = os.path.dirname(os.path.abspath(__file__))
        folder_path = os.path.abspath(os.path.join(script_directory, "../data/metadata_loads"))

        file_path, file_name= create_metadata_file(flat_uploads, current_time= current_time, metadata_folder_path= folder_path )

        path = f"{target_path}/{file_name}"

        service_client= get_service_client_token_credential(storage_name)
        directory_client = service_client.get_file_client(file_system= container, file_path= path)

        with open(file=file_path, mode="rb") as data:
            directory_client.upload_data(data, overwrite=True)
    
    transactions_paths = upload_transactions_csv("stgfull1","../data/", "files/transactions")
    products_paths = upload_products_csv("stgfull1","../data/", "files/products")
    locations_paths = upload_locations_csv("stgfull1","../data/", "files/locations")

    uploads_csv = [transactions_paths,products_paths,locations_paths]

    upload_metadata(uploads_csv, "stgfull1", "files/metadata","airdb2")
test_dag = test()
