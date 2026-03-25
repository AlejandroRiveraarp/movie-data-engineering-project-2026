# Databricks notebook source
spark.read.option("header", True).csv("abfss://bronce@prueba1db.dfs.core.windows.net/2024-12-16/movie_company").createOrReplaceTempView("v_movie_company_1")

# COMMAND ----------

# MAGIC %sql SELECT COUNT(1) FROM v_movie_company_1

# COMMAND ----------

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

movie_company_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("companyID", IntegerType(), True)
])
display(movie_company_schema)


# COMMAND ----------

movie_company_df = spark.read \
    .schema(movie_company_schema) \
    .csv(f"{bronze_folder_path}/{v_file_date}/movie_company")
    

movie_company_df.printSchema()
display(movie_company_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

movie_company_final_df = movie_company_df \
    .withColumnRenamed("movieId", "movie_id") \
    .withColumnRenamed("companyID", "company_id") \

movie_company_final_df = add_ingestion_Date(movie_company_final_df)\
    .withColumn("environment", lit(v_enviroment))\
        .withColumn("file_date", lit(v_file_date))

display(movie_company_final_df)

# COMMAND ----------

#overwrite_partitions("databricks_course_ws2.movie_silver","movie_company","file_date",v_file_date)

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists("movie_silver.movie_company"):
    

    deltaTable = DeltaTable.forName(spark, 'movie_silver.movie_company')

    deltaTable.alias('tgt') \
    .merge(
        movie_company_final_df.alias('src'),
        'tgt.movie_id = src.movie_id AND tgt.company_id = src.company_id AND tgt.file_date = src.file_date'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
    



else:
    movie_company_final_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("file_date") \
        .saveAsTable("movie_silver.movie_company")






# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT file_date, COUNT(1)
# MAGIC FROM movie_silver.movie_company
# MAGIC GROUP BY file_date;