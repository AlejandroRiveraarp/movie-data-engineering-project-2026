# Databricks notebook source


# COMMAND ----------

# MAGIC %run ./00_config_adls

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("p_enviroment","")
v_enviroment = dbutils.widgets.get("p_enviroment")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2024-12-16")

v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ingestion del archivo JSON Country.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 1 - Leer el archivo JSON usando "DataFrameReader" de spark

# COMMAND ----------

country_schema = "countryId INT, countryIsoCode STRING, countryName STRING"

display(country_schema)
 

# COMMAND ----------

countries_df = spark.read \
    .schema(country_schema) \
    .json(f"{bronze_folder_path}/{v_file_date}/country.json")
    





# COMMAND ----------

display(countries_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 2 - Eliminar columnas no deseadas del DataFrame 

# COMMAND ----------

countries_droped_df = countries_df.drop("countryIsoCode")
display(countries_droped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 3 - Cambiar el nombre de las columnas y añadir "Ingestion_date" y "enviroment"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

countries_final_df = countries_droped_df \
    .withColumnRenamed("countryId", "country_id")\
    .withColumnRenamed("countryName", "country_name")\
        .withColumn("file_date", lit(v_file_date))
    

display(countries_final_df)





# COMMAND ----------

countries_final_df = add_ingestion_Date(countries_final_df) \
    .withColumn("enviroment", lit(v_enviroment))

display(countries_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 4 - Escribir salida en formato "Parquet"

# COMMAND ----------

#overwrite_partitions("databricks_course_ws2.movie_silver","country","file_date",v_file_date)

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists("movie_silver.country"):
    

    deltaTable = DeltaTable.forName(spark, 'movie_silver.country')

    deltaTable.alias('tgt') \
    .merge(
        countries_final_df.alias('src'),
        'tgt.country_id = src.country_id AND tgt.file_date = src.file_date'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
    



else:
    countries_final_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("file_date") \
        .saveAsTable("movie_silver.country")




# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT file_date, COUNT(1)
# MAGIC FROM movie_silver.country
# MAGIC GROUP BY file_date;

# COMMAND ----------

display(spark.read.table("databricks_course_ws2.movie_silver.country"))

# COMMAND ----------

