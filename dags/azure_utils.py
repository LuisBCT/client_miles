from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
import os
import sys
from typing import Tuple, List

def get_service_client_token_credential(account_name) -> DataLakeServiceClient:
    #Create the client to connect
    account_url = f"https://{account_name}.dfs.core.windows.net"
    token_credential = DefaultAzureCredential() # Automatically use the env varaibles

    service_client = DataLakeServiceClient(account_url, credential=token_credential)

    return service_client

def upload_file_azure(local_file_name:str, target_path:str, current_time:str, file_system_name:str, service_client:DataLakeServiceClient):
    azure_file_name = f"{local_file_name.split('.')[0]}_{current_time}.csv"
    path = f"{target_path}/{azure_file_name}" # the path in the container

    directory_client = service_client.get_file_client(file_system= file_system_name, file_path= path)

    file_path = r"data/{}".format(local_file_name) #files can be in diferents folder within the container

    absolute_path = os.path.abspath(file_path)
    print(f"uploading file {azure_file_name} ....")
    with open(file=absolute_path, mode="rb") as data:
        directory_client.upload_data(data, overwrite=True)
    return path

def create_metadata_file(loads:list, current_time:str, metadata_folder_path: str) -> Tuple[str, str]:
    os.makedirs(metadata_folder_path, exist_ok=True)
    metadata_name = f"loads_{current_time}.txt"
    metadata_loads_file_path = os.path.join(metadata_folder_path, metadata_name)

    file_paths_string = "\n".join(loads) # create a string with the name of all the files uploaded

    with open(metadata_loads_file_path, "w") as metadata_loads_file:
        metadata_loads_file.write(file_paths_string)
    return metadata_loads_file_path,metadata_name