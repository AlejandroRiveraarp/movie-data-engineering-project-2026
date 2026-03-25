# Databricks notebook source


# COMMAND ----------

# MAGIC %run ./00_config_adls

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

spark.read.option("header", True).csv("abfss://bronce@prueba1db.dfs.core.windows.net/2024-12-23/genre.csv").createOrReplaceTempView("v_genres_1")

# COMMAND ----------

# MAGIC %sql SELECT COUNT(1) FROM v_genres_1 

# COMMAND ----------

dbutils.widgets.help()


# COMMAND ----------

dbutils.widgets.text("p_enviroment","")
v_enviroment = dbutils.widgets.get("p_enviroment")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2024-12-16")

v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# MAGIC %md
# MAGIC Ingestion del archivo "genre.csv" 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1 - Leer el archivo usando "DataFrameReader" de spark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType; 
from pyspark.sql.functions import col, lit, to_timestamp

# COMMAND ----------

language_schema = StructType(fields = [
    StructField('genreId', IntegerType(), False),
    StructField('genreName', StringType(), True),
])
display(language_schema)

# COMMAND ----------

genre_df = spark.read \
    .option("header", True) \
    .schema(language_schema) \
    .csv(f"{bronze_folder_path}/{v_file_date}/genre.csv")

display(genre_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Paso 2 - Cambiar nombre de columnas segun lo requerido 

# COMMAND ----------

genres_df_rename = genre_df.withColumnRenamed("genreId", "genre_id").withColumnRenamed("genreName", "genre_name")
display(genres_df_rename)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 3 - Agregar columnas "ingesta_date" y "enviromen"al DataFrame. 

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp




# COMMAND ----------

genre_df_final = add_ingestion_Date(genres_df_rename) \
    .withColumn("enviromen", lit(v_enviroment)) \
        .withColumn("file_date", lit(v_file_date))
display(genre_df_final)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 4 - Escribir en el Datalake en formato "Parket" a Delta

# COMMAND ----------

overwrite_partitions("databricks_course_ws2.movie_silver", "genre", "file_date", v_file_date)

# COMMAND ----------

# Guardamos de forma ADMINISTRADA (Sin el option path)
genre_df_final.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("file_date") \
    .saveAsTable("databricks_course_ws2.movie_silver.genre")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT file_date, COUNT(1)
# MAGIC FROM movie_silver.genre
# MAGIC GROUP BY file_date;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "abfss://silver@prueba1db.dfs.core.windows.net/genre_external_test"

# COMMAND ----------

display(spark.read.format("delta").load("abfss://silver@prueba1db.dfs.core.windows.net/genre_external_test"))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

