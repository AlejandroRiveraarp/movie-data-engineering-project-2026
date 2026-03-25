# Databricks notebook source
bronze_folder_path = "abfss://bronce@prueba1db.dfs.core.windows.net/"
silver_folder_path = "abfss://silver@prueba1db.dfs.core.windows.net/"
gold_folder_path   = "abfss://gold@prueba1db.dfs.core.windows.net/"


# COMMAND ----------

# MAGIC %run ../ingestion/00_config_adls