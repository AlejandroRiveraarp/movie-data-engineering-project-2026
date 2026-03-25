# Databricks notebook source
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC #### # --- PASO 1: CARGA DE FUNCIONES Y PARÁMETROS ---

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

# Definición de widgets y captura de fecha
dbutils.widgets.text("p_file_date", "2024-12-30")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import sum, desc, dense_rank, lit, col
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC #### # --- PASO 2: CARGA DE TABLAS SILVER ---

# COMMAND ----------


# Cargamos las tablas aplicando el filtro de fecha de una vez
movies_df = spark.read.table("movie_silver.movies") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

genres_df = spark.read.table("movie_silver.genre")

# COMMAND ----------

movies_genre_df = spark.read.table("movie_silver.movie_genre") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

# MAGIC %md
# MAGIC #### # --- PASO 3: TRANSFORMACIÓN Y JOINS ---

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join "genres" y "movies_genres"

# COMMAND ----------

# Join entre Géneros y su relación (usamos "genre_id" entre comillas para evitar duplicados de columna)
# Aplicamos dropDuplicates para asegurar que el cálculo de SUM sea exacto
genres_mov_gen_df = genres_df.join(movies_genre_df, "genre_id", "inner") \
    .select("genre_name", "movie_id") \
    .dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC #### join movies_df y genres_mov_gen_df 

# COMMAND ----------

# MAGIC %md
# MAGIC - filtrar peliculas en que su fecha de lanzamiento sea mayor o igual a 2016

# COMMAND ----------

# Filtrar películas con año >= 2015
movies_df_filter = movies_df.filter("year_release_date >= 2015")

# COMMAND ----------

# Join Maestro (usamos ["movie_id"] para evitar errores de referencia ambigua)
result_movies_genres_df = movies_df_filter.join(genres_mov_gen_df, ["movie_id"], "inner")






# COMMAND ----------

# Selección de columnas base para el reporte
result_df = result_movies_genres_df.select("year_release_date", "genre_name", "budget", "revenue")

# COMMAND ----------

from pyspark.sql.functions import sum
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, dense_rank,rank,asc,desc

# COMMAND ----------

# MAGIC %md
# MAGIC #### # --- PASO 4: AGREGACIONES Y RANKING (Lógica Gold) ---

# COMMAND ----------

# Agrupamos por año y género para sumar presupuestos y ganancias
results_groupby_df = result_df \
    .groupBy("year_release_date", "genre_name") \
    .agg(
        sum("budget").alias("total_budget"),
        sum("revenue").alias("total_revenue")
    )

# COMMAND ----------

# Definimos la ventana de ranking por año (el top de cada año)
window_spec = Window.partitionBy("year_release_date").orderBy(
    desc("total_budget"),
    desc("total_revenue")
)

# COMMAND ----------

# Aplicamos el ranking y agregamos la columna de partición create_date
final_df = results_groupby_df \
    .withColumn("rank", dense_rank().over(window_spec)) \
    .withColumn("create_date", lit(v_file_date)) # Columna clave para la partición
display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### # --- PASO 5: ESCRITURA EN CAPA GOLD ---

# COMMAND ----------

# MAGIC %md
# MAGIC #### datos en el data lake en formato delta

# COMMAND ----------

# 1. Limpiamos la partición en la tabla destino (usando tu función common)
#overwrite_partitions("movie_gold", "results_genres_ranking", "create_date", v_file_date)

# COMMAND ----------

from delta.tables import DeltaTable 
from pyspark.sql.functions import to_date, lit

# 1. Creamos la columna de fecha
result_df = result_df.withColumn("create_date", to_date(lit(v_file_date)))

# 2. CRÍTICO: Eliminamos duplicados basados en tu llave del MERGE
# Esto asegura que solo haya una fila por género/año para el ranking
result_df_cleaned = result_df.dropDuplicates(["year_release_date", "genre_name"])

if spark.catalog.tableExists("movie_gold.results_genres_ranking"):
    deltaTable = DeltaTable.forName(spark, 'movie_gold.results_genres_ranking')

    deltaTable.alias('tgt') \
    .merge(
        result_df_cleaned.alias('src'), # Usamos el DF limpio
        'tgt.year_release_date = src.year_release_date AND tgt.genre_name = src.genre_name'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

else:
    # La primera vez se escribe todo el ranking
    result_df_cleaned.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("create_date") \
        .saveAsTable("movie_gold.results_genres_ranking")

print(f"¡Carga de {v_file_date} finalizada!")





# COMMAND ----------

# MAGIC %sql SELECT * FROM movie_gold.results_genres_ranking;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT create_date, COUNT(1) as total_registros
# MAGIC FROM movie_gold.results_genres_ranking
# MAGIC GROUP BY create_date;