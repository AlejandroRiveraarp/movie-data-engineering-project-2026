# Databricks notebook source


# COMMAND ----------

spark.read.option("header", True).csv("abfss://bronce@prueba1db.dfs.core.windows.net/2024-12-16/language_role.json").createOrReplaceTempView("v_language_role_1")

# COMMAND ----------

# MAGIC %sql SELECT COUNT(1) FROM v_language_role_1

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
# MAGIC Configuracion para leer multiarchivos JSON en multilinea 

# COMMAND ----------

from pyspark.sql.functions import col

movie_languague_role_df = spark.read \
    .option("multiLine", True) \
    .json(f"{bronze_folder_path}/{v_file_date}/language_role.json")

movie_languague_role_df.printSchema()
display(movie_languague_role_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

movie_languague_role_final_df = movie_languague_role_df \
    .withColumnRenamed("languageRole", "language_role") \
    .withColumnRenamed("roleId", "role_id") \
        


movie_languague_role_final_df = add_ingestion_Date(movie_languague_role_final_df)\
    .withColumn("environment", lit(v_enviroment))\
        .withColumn("file_date", lit(v_file_date))

display(movie_languague_role_final_df)

# COMMAND ----------

overwrite_partitions("databricks_course_ws2.movie_silver","language_role","file_date",v_file_date)

# COMMAND ----------


movie_languague_role_final_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("file_date")\
    .saveAsTable("databricks_course_ws2.movie_silver.language_role")





# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT file_date, COUNT(1)
# MAGIC FROM movie_silver.language_role
# MAGIC GROUP BY file_date;