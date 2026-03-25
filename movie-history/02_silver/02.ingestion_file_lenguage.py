# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingestion de archivo lenguage.csv

# COMMAND ----------

spark.read.option("header", True).csv("abfss://bronce@prueba1db.dfs.core.windows.net/2024-12-23/language.csv").createOrReplaceTempView("v_language_1")

# COMMAND ----------

# MAGIC %sql SELECT COUNT(1) FROM v_language_1 

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

# MAGIC %run ../ingestion/00_config_adls

# COMMAND ----------


dbutils.widgets.help()
dbutils.widgets.text("p_enviroment","")
v_enviroment = dbutils.widgets.get("p_enviroment")

# COMMAND ----------

v_enviroment

# COMMAND ----------

dbutils.widgets.text("p_file_date","2024-12-16")

v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 1 - Leer el archivo CSV usan "DadaFrameReader" de spark 

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

language_schema = StructType(fields = [
    StructField('languageID', IntegerType(), False),
    StructField('languageCode', StringType(), False),
    StructField('languageName', StringType(), True)
])


# COMMAND ----------

language_df = spark.read \
    .option("header", True) \
    .schema(language_schema) \
    .csv(f"{bronze_folder_path}/{v_file_date}/language.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Paso 2 - Seleccionar solo columnas requeridas 

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

languaje_selected_df = language_df.select(col('languageID'), col('languageName'))
display(languaje_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 3 - Cambiar el nombre a las columnas segun lo requerido.

# COMMAND ----------

languaje_rename_df = languaje_selected_df.withColumnsRenamed({'languageID': 'language_Id', "languageName": 'language_name'})
display(languaje_rename_df)


# COMMAND ----------

# MAGIC %md
# MAGIC paso 4 - Agregar columnas "Ingestion_date" y "Enviroment" al data frame.

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

# COMMAND ----------

languanje_final_df = add_ingestion_Date(languaje_rename_df) \
    .withColumn("enviroment", lit(v_enviroment))\
        .withColumn("file_date", lit(v_file_date))

display(languanje_final_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Paso 5 - Escribir datos en el data lake en formato "parquet"

# COMMAND ----------

overwrite_partitions("databricks_course_ws2.movie_silver","languages","file_date",v_file_date)

# COMMAND ----------

languanje_final_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("file_date") \
    .saveAsTable("databricks_course_ws2.movie_silver.languages")

   



# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT file_date, COUNT(1)
# MAGIC FROM movie_silver.languages
# MAGIC GROUP BY file_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_silver.languages;

# COMMAND ----------

display(languanje_final_df)


# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE STORAGE CREDENTIAL databricks_course_ws2;

# COMMAND ----------

display(spark.read.table("databricks_course_ws2.movie_silver.languages"))