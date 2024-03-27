# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType
from pyspark.sql.functions import lit
import csv
from datetime import datetime
from functools import reduce as freduce

# COMMAND ----------

catalog_lz = "cat_airmiles"
schema_landing = "landing"

# COMMAND ----------

current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

lst = dbutils.fs.ls("/Volumes/cat_airmiles/landing/files/metadata")
latest_file = sorted(lst, reverse= True)[0]
path_metadata = "/Volumes/cat_airmiles/landing/files/metadata/"+latest_file.name

# COMMAND ----------

with open(path_metadata,"r") as data:
    content = data.read().splitlines()

    file_paths = ["/Volumes/cat_airmiles/landing/" + file_path for file_path in content]
    file_dict = {
                "transactions": [file_path for file_path in file_paths if "trans" in file_path],
                "locations": [file_path for file_path in file_paths if "location" in file_path],
                "products": [file_path for file_path in file_paths if "product" in file_path]
            }

# COMMAND ----------

def get_headers_csv(path):
    with open(path, 'r', newline='', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)  # Reads the first row (headers)
    return headers

def create_schema(header):
    header = [col if col != 'trans_id' else 'trans_key' for col in header]
    fields = [
        StructField(col, StringType(), True) 
        if col not in ['sales', 'units','store_num'] else StructField(col, FloatType() 
                                                            if col in ['sales'] else IntegerType(),True) 
        for col in header
        ]
    return StructType(fields)

# COMMAND ----------

schemas = {
        key: [
            create_schema(get_headers_csv(file_path)) for file_path in file_paths
        ]
        for key, file_paths in file_dict.items()
    }

# COMMAND ----------

def get_csv_df(schemas, files, columns):#create dataframes from csvs
    dfs = []
    for schema, file in zip(schemas, files):
        df = spark.read.format("csv").option("header", True).option("inferSchema", False).schema(schema).load(file).select(*columns)
        dfs.append(df)
    df = freduce(lambda df1, df2: df1.union(df2), dfs)
    return df

# COMMAND ----------

trans_columns = ["store_location_key","product_key","collector_key","trans_dt", "sales","units","trans_key"]
location_columns = ["store_location_key","region","province","city","postal_code","banner","store_num"]
product_columns = ["product_key","sku","upc","item_name","item_description","department","category"]

trans_df = get_csv_df(schemas["transactions"],file_dict["transactions"],trans_columns)
location_df = get_csv_df(schemas["locations"],file_dict["locations"],location_columns)
product_df = get_csv_df(schemas["products"],file_dict["products"],product_columns)

# COMMAND ----------

trans_df = trans_df.withColumn("Insert_date", lit(current_time).cast("timestamp"))
location_df = location_df.withColumn("Insert_date", lit(current_time).cast("timestamp"))
product_df = product_df.withColumn("Insert_date", lit(current_time).cast("timestamp"))

# COMMAND ----------

trans_df.write.mode("append").saveAsTable(f"{catalog_lz}.{schema_landing}.transactions")
location_df.write.mode("append").saveAsTable(f"{catalog_lz}.{schema_landing}.locations")
product_df.write.mode("append").saveAsTable(f"{catalog_lz}.{schema_landing}.products")
