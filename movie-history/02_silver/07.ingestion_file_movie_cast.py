# Databricks notebook source
spark.read.option("header", True).json("abfss://bronce@prueba1db.dfs.core.windows.net/2024-12-16/movie_cast.json").createOrReplaceTempView("v_movie_cast_1")

# COMMAND ----------

# MAGIC %sql SELECT COUNT(1) FROM v_movie_cast_1

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

from pyspark.sql.types import StringType, StructField, StructType, IntegerType 

# COMMAND ----------

from pyspark.sql.functions import col

movie_cast_df = spark.read \
    .option("multiLine", True) \
    .json(f"{bronze_folder_path}/{v_file_date}/movie_cast.json")
    

movie_cast_df.printSchema()
display(movie_cast_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

movie_cast_final_df = movie_cast_df \
    .withColumnRenamed("movieId", "movie_id") \
    .withColumnRenamed("personId", "person_id") \
    .withColumnRenamed("characterName", "character_name") \
    .withColumnRenamed("genderId", "gender_id") \
    .withColumnRenamed("castOrder", "cast_order") \
    
movie_cast_final_df =  add_ingestion_Date(movie_cast_final_df)\
    .withColumn("environment", lit(v_enviroment))\
        .withColumn("file_date", lit(v_file_date))

display(movie_cast_final_df)


# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

movie_cast_final_drop_df = movie_cast_final_df.drop("gender_id", "cast_order")

display(movie_cast_final_drop_df)

# COMMAND ----------

#overwrite_partitions("databricks_course_ws2.movie_silver","movie_cast","file_date",v_file_date)

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists("movie_silver.movie_cast"):
    

    deltaTable = DeltaTable.forName(spark, 'movie_silver.movie_cast')

    deltaTable.alias('tgt') \
    .merge(
        movie_cast_final_drop_df.alias('src'),
        'tgt.movie_id = src.movie_id AND tgt.person_id = src.person_id AND tgt.file_date = src.file_date'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
    



else:
    movie_cast_final_drop_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("file_date") \
        .saveAsTable("movie_silver.movie_cast")





# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT file_date, COUNT(1)
# MAGIC FROM movie_silver.movie_cast
# MAGIC GROUP BY file_date;

# COMMAND ----------

