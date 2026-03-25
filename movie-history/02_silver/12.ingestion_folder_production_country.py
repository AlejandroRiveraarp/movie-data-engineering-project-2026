# Databricks notebook source
# MAGIC %md
# MAGIC ingestion de la carpeta "produccion_country" 

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

production_country_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("countryId", IntegerType(), True),
    
])
display(production_country_schema)


# COMMAND ----------

# MAGIC %md
# MAGIC Formato JSON Multilinea 

# COMMAND ----------

production_country_df = spark.read \
    .schema(production_country_schema) \
    .option("multiLine", True) \
    .json(f"{bronze_folder_path}/{v_file_date}/production_country")
    

production_country_df.printSchema()
display(production_country_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

production_country_df_final = production_country_df \
    .withColumnRenamed("movieId", "movie_id") \
    .withColumnRenamed("countryId", "country_id") \

production_country_df_final = add_ingestion_Date(production_country_df_final)\
    .withColumn("environment", lit(v_enviroment))\
        .withColumn("file_date", lit(v_file_date))

display(production_country_df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC Exportamos en formato parquet

# COMMAND ----------

#overwrite_partitions("databricks_course_ws2.movie_silver","production_country","file_date",v_file_date)

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists("movie_silver.production_country"):
    

    deltaTable = DeltaTable.forName(spark, 'movie_silver.production_country')

    deltaTable.alias('tgt') \
    .merge(
        production_country_df_final.alias('src'),
        'tgt.movie_id = src.movie_id AND tgt.country_id = src.country_id AND tgt.file_date = src.file_date'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
    



else:
    production_country_df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("file_date") \
        .saveAsTable("movie_silver.production_country")





# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT file_date, COUNT(1)
# MAGIC FROM movie_silver.production_country
# MAGIC GROUP BY file_date;