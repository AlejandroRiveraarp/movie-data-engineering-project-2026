# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_Date(input_df):
  output_df = input_df.withColumn("ingestion_date", current_timestamp())
  return output_df

 

# COMMAND ----------

def overwrite_partitions(db_name,table_name,column_partition,file_date):
    if spark.catalog.tableExists(f"{db_name}.{table_name}"):
        spark.sql(f"DELETE FROM {db_name}.{table_name} WHERE {column_partition} = '{file_date}'")

# COMMAND ----------

bronze_folder_path = "abfss://bronce@prueba1db.dfs.core.windows.net/"
silver_folder_path = "abfss://silver@prueba1db.dfs.core.windows.net/"
gold_folder_path   = "abfss://gold@prueba1db.dfs.core.windows.net/"

# COMMAND ----------

