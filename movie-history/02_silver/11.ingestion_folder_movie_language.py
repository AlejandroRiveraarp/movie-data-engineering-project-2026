# Databricks notebook source
# MAGIC %md
# MAGIC ingestion de la carpeta "produccion_company" 

# COMMAND ----------

# MAGIC %run ./00_config_adls

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("p_enviroment", "")
dbutils.widgets.get("p_enviroment")
v_enviroment = dbutils.widgets.get("p_enviroment")

    

# COMMAND ----------

dbutils.widgets.text("p_file_date","2024-12-16")

v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 1 - Leer los archivos CSV usando "DataFrameReader" de spark 

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

movie_language_schema = StructType([
    StructField("movieId", StringType(), True),
    StructField("languageId", StringType(), True),
    StructField("languageRoleID", IntegerType(), True)
])
display(movie_language_schema)


# COMMAND ----------

# MAGIC %md
# MAGIC Formato JSON Multilinea 

# COMMAND ----------

movie_language_df = spark.read \
    .option("multiLine", True) \
    .json(f"{bronze_folder_path}/{v_file_date}/movie_language")
    

movie_language_df.printSchema()
display(movie_language_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

movie_language_final_df = movie_language_df \
    .withColumnRenamed("movieId", "movie_id") \
    .withColumnRenamed("languageId", "language_id") \
    .withColumnRenamed("languageRoleID", "languageRole_id") \

movie_language_final_df = add_ingestion_Date(movie_language_final_df)\
    .withColumn("environment", lit(v_enviroment))\
        .withColumn("file_date", lit(v_file_date))


display(movie_language_final_df)

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

# MAGIC %md
# MAGIC Drop eliminamos una columna 

# COMMAND ----------

movie_language_drop_df = movie_language_final_df.drop(col("languageRole_ID"))
display(movie_language_drop_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Exportamos en formato parquet

# COMMAND ----------

#overwrite_partitions("databricks_course_ws2.movie_silver","movie_language","file_date",v_file_date)

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists("movie_silver.movie_language"):
    

    deltaTable = DeltaTable.forName(spark, 'movie_silver.movie_language')

    deltaTable.alias('tgt') \
    .merge(
        movie_language_drop_df.alias('src'),
        'tgt.movie_id = src.movie_id AND tgt.language_id = src.language_id AND tgt.file_date = src.file_date'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
    



else:
    movie_language_drop_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("file_date") \
        .saveAsTable("movie_silver.movie_language")






# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT file_date, COUNT(1)
# MAGIC FROM movie_silver.movie_language
# MAGIC GROUP BY file_date;