# Databricks notebook source
spark.read.option("header", True).csv("abfss://bronce@prueba1db.dfs.core.windows.net/2024-12-16/production_company").createOrReplaceTempView("v_production_company_1")

# COMMAND ----------

# MAGIC %sql SELECT COUNT(1) FROM v_production_company_1

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

production_company_schema = StructType([
    StructField("companyId", IntegerType(), True),
    StructField("companyName", StringType(), True)
])
display(production_company_schema)


# COMMAND ----------

production_company_df = spark.read \
    .schema(production_company_schema) \
    .csv(f"{bronze_folder_path}/{v_file_date}/production_company")
    

production_company_df.printSchema()
display(production_company_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

production_company_final_df = production_company_df \
    .withColumnRenamed("companyId", "company_id") \
    .withColumnRenamed("companyName", "company_name") \

production_company_final_df = add_ingestion_Date(production_company_final_df)\
    .withColumn("environment", lit(v_enviroment))\
        .withColumn("file_date", lit(v_file_date))

display(production_company_final_df)

# COMMAND ----------

#overwrite_partitions("databricks_course_ws2.movie_silver","production_company","file_date",v_file_date)

# COMMAND ----------


from delta.tables import DeltaTable
if spark.catalog.tableExists("movie_silver.production_company"):
    

    deltaTable = DeltaTable.forName(spark, 'movie_silver.production_company')

    deltaTable.alias('tgt') \
    .merge(
        production_company_final_df.alias('src'),
        'tgt.company_id = src.company_id AND tgt.file_date = src.file_date'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
    



else:
    production_company_final_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("file_date") \
        .saveAsTable("movie_silver.production_company")







# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT file_date, COUNT(1)
# MAGIC FROM movie_silver.production_company
# MAGIC GROUP BY file_date;

# COMMAND ----------

# MAGIC %md
# MAGIC #### para leer archivos de data lake, container cuando lo escribimos en tabla externa no administrada.
# MAGIC display(spark.read.parquet("abfss://silver@prueba1db.dfs.core.windows.net/production_company/"))

# COMMAND ----------

# MAGIC %md
# MAGIC