# Databricks notebook source
spark.read.option("header", True).csv("abfss://bronce@prueba1db.dfs.core.windows.net/2024-12-16/movie_genre.json").createOrReplaceTempView("v_genre_1")

# COMMAND ----------

# MAGIC %sql SELECT COUNT(1) FROM v_genre_1

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
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 1 - leer data

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType, IntegerType

# COMMAND ----------

genre_schema = StructType(fields = [
    StructField("movieId", IntegerType(), True),
    StructField("genreId", IntegerType(), True)
])

# COMMAND ----------

genre_df = spark.read \
    .schema(genre_schema) \
    .json(f"{bronze_folder_path}/{v_file_date}/movie_genre.json")
display(genre_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 2 - Renombrar columnas y añadir nuevas columnas
# MAGIC 1 - "genreID" renombrar a "genre_id" y "movieID" a "movie_id"
# MAGIC
# MAGIC 2 - Agregar columnas "ingestion_date" y "enviroment"
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

rename_genre_df = genre_df \
    .withColumnRenamed("movieId", "movie_id") \
    .withColumnRenamed("genreId", "genre_id") \
    
rename_genre_df =  add_ingestion_Date(rename_genre_df)\
    .withColumn("enviroment", lit(v_enviroment))\
        .withColumn("file_date", lit(v_file_date))



display(rename_genre_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 3 - Escribir salida en formato parket y particionar en movie_id .partitionBy

# COMMAND ----------

#overwrite_partitions("databricks_course_ws2.movie_silver","movie_genre","file_date",v_file_date)

# COMMAND ----------


from delta.tables import DeltaTable
if spark.catalog.tableExists("movie_silver.movie_genre"):
    

    deltaTable = DeltaTable.forName(spark, 'movie_silver.movie_genre')

    deltaTable.alias('tgt') \
    .merge(
        rename_genre_df.alias('src'),
        'tgt.movie_id = src.movie_id AND tgt.genre_id = src.genre_id AND tgt.file_date = src.file_date'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
    



else:
    rename_genre_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("file_date") \
        .saveAsTable("movie_silver.movie_genre")





# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT file_date, COUNT(1)
# MAGIC FROM movie_silver.movie_genre
# MAGIC GROUP BY file_date;

# COMMAND ----------

