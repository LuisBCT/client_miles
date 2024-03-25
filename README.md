# Project Description
The goal of this project is to upload local CSV files to Azure storage and then process them using Azure Databricks to create corresponding tables and layers. For orchestrating the upload from local to cloud, Airflow will be utilized, while processing orchestration will be handled through workflows in Databricks. Additionally, a simple dashboard will be created within Databricks for visualization and analysis purposes

## Tools
- Python
- Airflow
- Azure
- Databricks
- Docker
- VScode

## Resource and Service Creation
To begin, a resource group was established, followed by the creation of key services including Azure Databricks, Storage account Gen2, and an Access connector specifically tailored for Databricks. This Access connector is for in setting up the metastore and leveraging the Unity Catalog effectively.

![image](https://github.com/LuisBCT/client_miles/assets/124119564/eea59634-6b7e-425d-8eb0-f1364c9a7552)

Also create a service principal with their secret that will be use to connect with the storage account 

![image](https://github.com/LuisBCT/client_miles/assets/124119564/2116f543-c79b-4dfa-8de4-a292becc9e1e)


## Setting up VScode workspace
Within VSCode, the necessary folders for Airflow usage were created, alongside the official [Docker-compose file](https://airflow.apache.org/docs/apache-airflow/2.8.1/docker-compose.yaml).  

![image](https://github.com/LuisBCT/client_miles/assets/124119564/3aec28cc-98ab-41ef-96f5-831b17dfab70)

Then a .env file was established to securely store environment variables required for authentication using `DefaultAzureCredential`.
![image](https://github.com/LuisBCT/client_miles/assets/124119564/8987f8fb-8e6e-4588-80b7-0111d3533a7a)

 Finally, modifications were made to the Docker-compose file, with particular focus on the following key aspects:

![image](https://github.com/LuisBCT/client_miles/assets/124119564/9c9e44a3-7b63-43e3-9a2c-ae25a1a33b5f)   
![image](https://github.com/LuisBCT/client_miles/assets/124119564/2b7b47cb-c36e-4868-9f3d-50b41d21e97d)

## Dag development
In this case the connection with the storage account was by *Microsoft Entra ID* according to the [documentation](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-directory-file-acl-python?tabs=azure-ad) the `DefaultAzureCredential` automatically will use the enviroment variables `AZURE_CLIENT_ID` `AZURE_TENANT_ID` `AZURE_CLIENT_SECRET` to access the storage account. So was defined the function `get_service_client_token_credential`





