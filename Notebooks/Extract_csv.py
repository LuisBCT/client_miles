# Databricks notebook source
# MAGIC %run ../Notebooks/Utils

# COMMAND ----------

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
path_volume = "/Volumes/cat_airmiles/landing/"

# COMMAND ----------

data_processor = DataProcessor(path_metadata,path_volume)
schemas = data_processor.get_schemas()
files = data_processor.get_paths_in_metadata()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Tables

# COMMAND ----------

trans_columns = ["store_location_key","product_key","collector_key","trans_dt", "sales","units","trans_key"]
location_columns = ["store_location_key","region","province","city","postal_code","banner","store_num"]
product_columns = ["product_key","sku","upc","item_name","item_description","department","category"]

trans_df = data_processor.get_csv_df(schemas["transactions"],files["transactions"],trans_columns)
location_df = data_processor.get_csv_df(schemas["locations"],files["locations"],location_columns)
product_df = data_processor.get_csv_df(schemas["products"],files["products"],product_columns)

# COMMAND ----------

trans_df = trans_df.withColumn("Insert_date", lit(current_time).cast("timestamp"))
location_df = location_df.withColumn("Insert_date", lit(current_time).cast("timestamp"))
product_df = product_df.withColumn("Insert_date", lit(current_time).cast("timestamp"))

# COMMAND ----------

trans_df.write.mode("append").saveAsTable(f"{catalog_lz}.{schema_landing}.transactions")
location_df.write.mode("append").saveAsTable(f"{catalog_lz}.{schema_landing}.locations")
product_df.write.mode("append").saveAsTable(f"{catalog_lz}.{schema_landing}.products")
