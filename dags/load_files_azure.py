import os
import sys
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
    def task1(storage_name):
        service_client= get_service_client_token_credential(storage_name)
        system_client= service_client.get_file_system_client(file_system="airdb2")
        files = system_client.get_paths(path = "", recursive= False)
        for p in files:
            print(p.name)
    
    task1("stgfull1")

test_dag = test()
