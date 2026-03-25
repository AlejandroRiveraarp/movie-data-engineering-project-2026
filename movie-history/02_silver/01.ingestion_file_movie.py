# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion del archivo movie.csv
# MAGIC

# COMMAND ----------

# Prueba rápida para ver si el "Principal" está activo
#try:
    #dbutils.notebook.run("01.ingestion_file_movie", 10, {"p_enviroment": "developer", "p_file_date": "2024-12-30"})
    #print("Conexión exitosa")
#except Exception as e:
    #print(f"El error persiste: {e}")

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("p_enviroment","")
v_enviroment = dbutils.widgets.get("p_enviroment")


# COMMAND ----------

dbutils.widgets.text("p_file_date","")

v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_enviroment

# COMMAND ----------

# MAGIC %md
# MAGIC ###Paso 1 - Leer el archivo CSV usando "DataFrameReader" de Spark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
#Librerias 

# COMMAND ----------

# MAGIC %md
# MAGIC Codigo para eliminar cache o carpetas.
# MAGIC dbutils.fs.rm(
# MAGIC     "abfss://silver@prueba1db.dfs.core.windows.net/movies/",
# MAGIC     recurse=True
# MAGIC )

# COMMAND ----------

#DataFramereader de Spark (Header & Schema)

df_movie_bronce = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(f"{bronze_folder_path}/{v_file_date}/movie.csv")

display(df_movie_bronce)


# COMMAND ----------

#Esquema de tabla movies
movie_schema = StructType(fields=[
    StructField("movieId", IntegerType(), False),
    StructField("title", StringType(), True),    
    StructField("budget", IntegerType(), True),
    StructField("homePage", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("popularity", StringType(), True),
    StructField("yearReleaseDate", StringType(), True),
    StructField("releaseDate", StringType(), True),
    StructField("revenue", StringType(), True),
    StructField("durationTime", StringType(), True),
    StructField("movieStatus", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("voteAverage", StringType(), True),
    StructField("voteCount", StringType(), True)
])


# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

movie_df = spark.read \
    .schema(movie_schema) \
    .option("header", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("multiLine", True) \
    .csv(f"{bronze_folder_path}/{v_file_date}/movie.csv")




# COMMAND ----------

movie_df = movie_df.withColumn(
    "releaseDate_clean",
    to_date("releaseDate", "yyyy-MM-dd")
).withColumn(
    "yearReleaseDate",
    year(col("releaseDate_clean"))
)



# COMMAND ----------

# MAGIC %md
# MAGIC ###Paso 2. Seleccionar solo las columnas requeridas.

# COMMAND ----------

from pyspark.sql.functions import col




# COMMAND ----------

#Seleccionar solo las columnas requeridas
movie_selected_df = movie_df.select(
    col("movieId"),
    col("title"),
    col("budget"),
    col("popularity"),
    col("releaseDate_clean"),
    col("revenue"),
    col("durationTime"),
    col("movieStatus"),
    col("tagline"),
    col("voteAverage"),
    col("voteCount"),
    col("yearReleaseDate")
)
display(movie_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Paso 3. cambiar el nombre a las columnas 

# COMMAND ----------

from pyspark.sql.functions import col



# COMMAND ----------

# Metodo 1 Renombrar columnas ejemplo: title -> Titutlo .alias("")

movie_rename_df = movie_selected_df \
    .withColumnRenamed("title", "title") \
    .withColumnRenamed("movieId", "movie_id") \
    .withColumnRenamed("releaseDate_clean", "release_date") \
    .withColumnRenamed("voteCount", "vote_count") \
    .withColumnRenamed("yearReleaseDate", "year_release_date") \
    .withColumnRenamed("movieStatus", "movie_status") \
    .withColumnRenamed("tagline", "tag_line") \
    .withColumnRenamed("voteAverage", "vote_average") \
    .withColumnRenamed("yearReleaseDate", "year_releaseDate") \
    .withColumnRenamed("durationTime", "duration_time")



display(movie_rename_df)






# COMMAND ----------

# MAGIC %md
# MAGIC ###Paso 4. agregar una columna 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

movies_final_df = add_ingestion_Date(movie_rename_df)\
    .withColumn("enviroment", lit(v_enviroment)) \
.withColumn("file_date", lit(v_file_date))


display(movies_final_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ###Paso 5. escribir datos en el datalake en formato packet

# COMMAND ----------

# MAGIC %md
# MAGIC if spark.catalog.tableExists("databricks_course_ws2.movie_silver.movies"):
# MAGIC     spark.sql(f"DELETE FROM databricks_course_ws2.movie_silver.movies WHERE file_date = '{v_file_date}'")

# COMMAND ----------

#overwrite_partitions("databricks_course_ws2.movie_silver","movies","file_date",v_file_date)

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists("movie_silver.movies"):
    

    deltaTable = DeltaTable.forName(spark, 'movie_silver.movies')

    deltaTable.alias('tgt') \
    .merge(
        movies_final_df.alias('src'),
        'tgt.movie_id = src.movie_id AND tgt.file_date = src.file_date'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
    



else:
    movies_final_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("file_date") \
        .saveAsTable("movie_silver.movies")

 




# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT file_date, COUNT(1)
# MAGIC FROM movie_silver.movies
# MAGIC GROUP BY file_date;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED movie_silver.movies;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESC EXTENDED movie_silver.movies;

# COMMAND ----------

movies_final_df.select("year_release_date").distinct().orderBy("year_release_date").show(20, False)


# COMMAND ----------

display(dbutils.fs.ls(f"{bronze_folder_path}/{v_file_date}/movie.csv"))


# COMMAND ----------

df = spark.read.csv(f"{bronze_folder_path}/{v_file_date}/movie.csv", header=True, inferSchema=True)
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")