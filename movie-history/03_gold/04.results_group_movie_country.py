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

from pyspark.sql.functions import sum, desc, dense_rank, lit, col
from pyspark.sql.window import Window

# COMMAND ----------

# Captura de fecha desde el widget
dbutils.widgets.text("p_file_date", "2024-12-30")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### # --- PASO 2: CARGA DE TABLAS SILVER ---

# COMMAND ----------

# Filtramos movies y relaciones por file_date para procesar solo lo nuevo
movies_df = spark.read.table("movie_silver.movies") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

country_df = spark.read.table("movie_silver.country")

# COMMAND ----------

production_country_df = spark.read.table("movie_silver.production_country") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

movie_company_df = spark.read.table("movie_silver.movie_company")

# COMMAND ----------

# MAGIC %md
# MAGIC #### # --- PASO 3: TRANSFORMACIONES Y JOINS ---

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join production_country_df and country_df 

# COMMAND ----------

# Mejorado: Join con sintaxis de comillas para evitar duplicidad de columnas
# Incluimos dropDuplicates para que los SUM en Gold no tengan errores de conteo doble
country_production_country_df = production_country_df.join(country_df, "country_id", "inner") \
    .select("country_name", "movie_id") \
    .dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC - Filtrar las peliculas donde su fecha de lanzamiento sea mayor o igual a 2015

# COMMAND ----------

# Filtrado de películas (Año >= 2015)
movie_filter_df = movies_df.filter("year_release_date >= 2015") \
    .select("year_release_date", "movie_id", "budget", "revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join movie_filter_df and  country_production_country_df  
# MAGIC

# COMMAND ----------

# Mejorado: Join usando ["movie_id"] para evitar errores de ambigüedad
results_joined_df = movie_filter_df.join(country_production_country_df, ["movie_id"], "inner")

display(results_joined_df)

# COMMAND ----------

# Selección de columnas base
results_df = results_joined_df.select("year_release_date", "country_name", "budget", "revenue")
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### # --- PASO 4: AGREGACIONES Y RANKING ---

# COMMAND ----------

from pyspark.sql.functions import sum 

# Agrupación por año y país
resut_group_by = results_df.groupBy("year_release_date", "country_name") \
    .agg(
        sum("budget").alias("total_budget"),
        sum("revenue").alias("total_revenue")
    )
display(resut_group_by)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Particion Rank

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, dense_rank


# Definición de la Ventana para el Ranking
result_window = Window.partitionBy("year_release_date").orderBy(
    desc("total_budget"),
    desc("total_revenue")
)





# COMMAND ----------

# Aplicación de Ranking y creación de columna de partición
final_df = resut_group_by \
    .withColumn("rank", dense_rank().over(result_window)) \
    .withColumn("create_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### # --- PASO 5: ESCRITURA EN CAPA GOLD (MÉTODO SEGURO) ---

# COMMAND ----------

# 1. AGREGAMOS EL IMPORT QUE FALTA (DeltaTable)
from delta.tables import DeltaTable 
from pyspark.sql.functions import to_date, lit, sum

# 2. Captura de fecha desde el widget (Asegúrate de que esta variable exista)
v_file_date = dbutils.widgets.get("p_file_date")

# 3. Aseguramos que la columna 'create_date' sea tipo Date
final_df = final_df.withColumn("create_date", to_date(lit(v_file_date)))

# Nombre de la tabla destino
target_table = "movie_gold.result_group_movie_country"

if spark.catalog.tableExists(target_table):
    # Ahora sí reconocerá DeltaTable gracias al import de arriba
    deltaTable = DeltaTable.forName(spark, target_table)

    deltaTable.alias('tgt') \
    .merge(
        final_df.alias('src'),
        'tgt.year_release_date = src.year_release_date AND \
         tgt.country_name = src.country_name AND \
         tgt.create_date = src.create_date'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

else:
    # Creación inicial
    final_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("create_date") \
        .saveAsTable(target_table)

print(f"¡Proceso Gold para {v_file_date} completado exitosamente!")


# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT create_date, COUNT(1) as total_registros
# MAGIC FROM movie_gold.result_group_movie_country
# MAGIC GROUP BY create_date;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM movie_gold.result_group_movie_country 
# MAGIC WHERE year_release_date = 2015 
# MAGIC ORDER BY rank ASC;

# COMMAND ----------

# MAGIC %sql SELECT * FROM movie_gold.result_group_movie_country;

# COMMAND ----------

# MAGIC %md
# MAGIC