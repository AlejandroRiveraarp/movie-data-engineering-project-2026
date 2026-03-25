# Databricks notebook source
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# 2. Vuélvelo a crear. Ahora sí debería tomar el default.
dbutils.widgets.text("p_file_date", "2024-12-30")

# 3. Captura el valor
v_file_date = dbutils.widgets.get("p_file_date")
print(f"La fecha capturada es: '{v_file_date}'")

# COMMAND ----------

 movies_df = spark.read.table("movie_silver.movies")\
                            .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

display(movies_df)

# COMMAND ----------

language_df = spark.read.table("movie_silver.languages")

# COMMAND ----------

movie_language_df = spark.read.table("movie_silver.movie_language")\
                            .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

genre_df = spark.read.table("movie_silver.genre")

# COMMAND ----------

movies_genre_df = spark.read.table("movie_silver.movie_genre")\
                            .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join entre "Language" "Movie_language"

# COMMAND ----------

language_df = language_df.withColumnRenamed("language_Id", "language_id")

# COMMAND ----------

languages_mov_langueage_df = language_df.join(movie_language_df, "language_id", "inner") \
    .select(language_df.language_name,language_df.language_id, movie_language_df.movie_id)
        

# COMMAND ----------

language_df.printSchema()
movie_language_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join Genres y "Movie_Genres"

# COMMAND ----------

# Join entre "Genres" y "Movie_Genres"
genres_mov_gen_df = genre_df.join(movies_genre_df, "genre_id", "inner") \
    .select(genre_df.genre_name,genre_df.genre_id, movies_genre_df.movie_id)

# COMMAND ----------

languages_clean_df = languages_mov_langueage_df.dropDuplicates(["movie_id", "language_name"])
genres_clean_df = genres_mov_gen_df.dropDuplicates(["movie_id", "genre_name"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join movies_df, languages_mov_langueage_df y genres_mov_gen_df

# COMMAND ----------

# MAGIC %md
# MAGIC - Filtrar peliculas donde su fecha de lanzamiento sea mayor a 2000

# COMMAND ----------

movie_filter_df = movies_df.filter("year_release_date >= 2000")

# COMMAND ----------



# 2. Join Final usando las versiones "CLEAN"
# Usamos ["movie_id"] para evitar columnas duplicadas con el mismo nombre
result_mov_genres_language_df = movie_filter_df \
    .join(languages_clean_df, ["movie_id"], "inner") \
    .join(genres_clean_df, ["movie_id"], "inner")

# 3. Verificación
print(f"Total filas en Gold: {result_mov_genres_language_df.count()}")


    

# COMMAND ----------

# MAGIC %md
# MAGIC - Agregar la columna "create_date" 

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

result_df = result_mov_genres_language_df \
    .select(
        movie_filter_df.movie_id,
        "language_id",
        "genre_id",
        "title", 
        "duration_time", 
        "release_date", 
        "vote_average", 
        "language_name", 
        "genre_name"
    ) \
    .withColumn("create_date", lit(v_file_date))
    

# COMMAND ----------

# MAGIC %md
# MAGIC - ordenar por la release_date de manera desendente 

# COMMAND ----------

print(f"3a. Registros en Movie_Language: {movie_language_df.count()}")
print(f"3b. Registros en Movie_Genre: {movies_genre_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Escribir datos en DataLake en formato "parquet"

# COMMAND ----------

print(f"Filas a insertar: {result_df.count()}")
print(f"Movies para esa fecha: {movies_df.count()}")
print(f"Relación Película-Lenguaje para esa fecha: {movie_language_df.count()}")
print(f"Relación Película-Género para esa fecha: {movies_genre_df.count()}")
print(f"Películas año >= 2000: {movies_df.filter('year_release_date >= 2000').count()}")

# COMMAND ----------

#overwrite_partitions("movie_gold", "results_mov_genres_language", "create_date", v_file_date)

# COMMAND ----------

from pyspark.sql.functions import to_date

# Convertimos create_date a solo fecha para evitar miles de carpetas
result_df = result_df.withColumn("create_date", to_date("create_date"))

if spark.catalog.tableExists("movie_gold.results_mov_genres_language"):
    deltaTable = DeltaTable.forName(spark, 'movie_gold.results_mov_genres_language')

    deltaTable.alias('tgt') \
    .merge(
        result_df.alias('src'),
        'tgt.movie_id = src.movie_id AND tgt.language_id = src.language_id AND tgt.genre_id = src.genre_id'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

else:
    # Ahora sí podemos particionar por create_date porque es solo YYYY-MM-DD
    result_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("create_date") \
        .saveAsTable("movie_gold.results_mov_genres_language")




print(f"¡Carga de {v_file_date} finalizada!")



# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT create_date, COUNT(1) as total_registros
# MAGIC FROM movie_gold.results_mov_genres_language
# MAGIC GROUP BY create_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE movie_gold.results_mov_genres_language;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_gold.results_mov_genres_language;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'movies' as tabla, COUNT(1) FROM movie_silver.movies
# MAGIC UNION ALL
# MAGIC SELECT 'genres' as tabla, COUNT(1) FROM movie_silver.movie_genre
# MAGIC UNION ALL
# MAGIC SELECT 'languages' as tabla, COUNT(1) FROM movie_silver.language_role;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT title, language_name, genre_name, create_date, COUNT(*) as cantidad
# MAGIC FROM movie_gold.results_mov_genres_language
# MAGIC GROUP BY title, language_name, genre_name, create_date
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY cantidad DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consulta A: Ver qué columnas tiene realmente la tabla de géneros
# MAGIC -- (Esto nos dirá si perdiste el ID de la película)
# MAGIC DESCRIBE movie_silver.genre;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consulta B: Ver si hay duplicados en Películas (Silver)
# MAGIC SELECT * FROM (
# MAGIC   SELECT *, COUNT(*) OVER(PARTITION BY movie_id, file_date) as conteo 
# MAGIC   FROM movie_silver.movies
# MAGIC ) WHERE conteo > 1;