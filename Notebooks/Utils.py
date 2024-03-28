# Databricks notebook source
import csv
from delta.tables import *
from functools import reduce as freduce

# COMMAND ----------

class DataProcessor:
    
    def __init__(self,general_path, volume_path):
        """
        Initializes the DataProcessor class with paths for general metadata and volume files.

        Args:
        - general_path (str): The path to the general metadata file.
        - volume_path (str): The path to the directory containing volume files.
        """
        self.general_path = general_path
        self.volume_path = volume_path
    
    def get_paths_in_metadata(self):
        """
        Retrieves file paths from the general metadata file and categorizes them based on type.

        Returns:
        - dict: A dictionary containing lists of file paths categorized by type ('transactions', 'locations', 'products').
        """
        with open(self.general_path,"r") as data:
            content = data.read().splitlines() # get the names of uploaded files
        
        file_paths = [self.volume_path + file_path for file_path in content]
        file_dict = {
                    "transactions": [file_path for file_path in file_paths if "trans" in file_path],
                    "locations": [file_path for file_path in file_paths if "location" in file_path],
                    "products": [file_path for file_path in file_paths if "product" in file_path]
                    } # separate the file names according the type
        return file_dict
    
    def get_headers_csv(self, path:str):
        """
        Retrieves the headers of a CSV file.

        Args:
        - path (str): The path to the CSV file.

        Returns:
        - list: A list of header column names.
        """
        with open(path, 'r', newline='', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            headers = next(csv_reader)  # Reads the first row (headers)
        return headers
    
    def create_schema(self, header):
        """
        Creates a schema based on the provided header for a DataFrame.

        Args:
        - header (list): The list of column names for the schema.

        Returns:
        - StructType: The schema for the DataFrame.
        """
        header = [col if col != 'trans_id' else 'trans_key' for col in header] # only the col "trans_id" will change to "trans_key"
        fields = [
            StructField(col, StringType(), True) 
            if col not in ['sales', 'units','store_num'] else StructField(col, FloatType() 
                                                                if col in ['sales'] else IntegerType(),True) 
            for col in header
            ] # create the schema for the specific headers
        return StructType(fields)
    
    def get_schemas(self):
        """
        Retrieves schemas for CSV files based on their headers.

        Returns:
        - dict: A dictionary containing schemas for each CSV file type.
        """
        files = self.get_paths_in_metadata()
        schemas = {
            key: [
                self.create_schema(self.get_headers_csv(file_path)) for file_path in file_paths
            ]
            for key, file_paths in files.items()
        } # create a dict that store schema of each csv file
        return schemas
    
    def get_csv_df(self, schemas, files, columns):
        """
        Creates DataFrames from CSV files based on provided schemas and columns.

        Args:
        - schemas (dict): A dictionary containing schemas for each CSV file type.
        - files (dict): A dictionary containing lists of file paths categorized by type.
        - columns (list): A list of column names to select for the DataFrames.

        Returns:
        - DataFrame: A DataFrame containing data from all CSV files.
        """
        dfs = []
        for schema, file in zip(schemas, files):
            df = spark.read.format("csv").option("header", True).option("inferSchema", False).schema(schema).load(file).select(*columns)
            dfs.append(df)
        df = freduce(lambda df1, df2: df1.union(df2), dfs) # if there are more that 1 df within the dfs list will union all to one
        return df
