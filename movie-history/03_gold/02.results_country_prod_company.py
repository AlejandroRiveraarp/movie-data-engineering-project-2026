# Databricks notebook source
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

from pyspark.sql.functions import lit, col

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

country_df = spark.read.table("movie_silver.country")

# COMMAND ----------

production_country_df = spark.read.table("movie_silver.production_country")\
                            .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

production_company_df = spark.read.table("movie_silver.production_company")\
                            .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

movie_company_df = spark.read.table("movie_silver.movie_company")

# COMMAND ----------

# --- PASO 1: Limpieza preventiva de duplicados en los DataFrames intermedios ---
# Esto asegura que cada relación película-país y película-compañía sea única
country_clean_df = production_country_df.join(country_df, "country_id", "inner") \
    .select("movie_id", "country_name") \
    .dropDuplicates()

company_clean_df = production_company_df.join(movie_company_df, "company_id", "inner") \
    .select("movie_id", "company_name") \
    .dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join country y production_country 

# COMMAND ----------

country_clean_df = country_df.join(production_country_df, 
                                   country_df.country_id == production_country_df.country_id, 
                                   "inner") \
    .select(
        production_country_df.movie_id, 
        country_df.country_id, 
        country_df.country_name
    ) \
    .dropDuplicates(["movie_id", "country_name"])

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join production_company y "movie_company"

# COMMAND ----------

# Join entre "production_company" y "movie_company" al estilo del profe
company_clean_df = production_company_df.join(movie_company_df, 
                                              production_company_df.company_id == movie_company_df.company_id, 
                                              "inner") \
    .select(
        production_company_df.company_name, 
        production_company_df.company_id, 
        movie_company_df.movie_id
    ) \
    .dropDuplicates(["movie_id", "company_name"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join "movies_df", "country_prod_coud_df" y  production_com_movie_copany_df

# COMMAND ----------

# MAGIC %md
# MAGIC - filtrar peliculas donde su fecha de  lanzamiento sea mayor o igual a 2010

# COMMAND ----------

movie_filter_df = movies_df.filter("year_release_date >= 2010")

# COMMAND ----------

result_df = movie_filter_df \
    .join(country_clean_df, ["movie_id"], "inner") \
    .join(company_clean_df, ["movie_id"], "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC agregar columna "create_date"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

result_final = result_df.select(
    movie_filter_df.movie_id,
    "country_id",
    "company_id",
    "title", 
    "budget", 
    "revenue", 
    "duration_time", 
    "release_date", 
    "country_name", 
    "company_name"
).withColumn("create_date", lit(v_file_date))

# 4. Ordenamos para que se vea bien
result_order_by_df = result_final.orderBy("title")



    

# COMMAND ----------

# MAGIC %md
# MAGIC - obtener por la columna title de manera ascendente 

# COMMAND ----------

#overwrite_partitions("movie_gold", "results_country_prod_company", "create_date", v_file_date)

# COMMAND ----------


from delta.tables import DeltaTable 
from pyspark.sql.functions import to_date, lit


result_df = result_df.withColumn("create_date", to_date(lit(v_file_date)))

if spark.catalog.tableExists("movie_gold.results_country_prod_company"):
    deltaTable = DeltaTable.forName(spark, 'movie_gold.results_country_prod_company')

    deltaTable.alias('tgt') \
    .merge(
        result_df.alias('src'),
        # Quitamos genre_id si no está en esta tabla específica, 
        # o asegúrate de que movie_id + country_id + company_id sean tu llave única
        'tgt.movie_id = src.movie_id AND tgt.country_id = src.country_id AND tgt.company_id = src.company_id'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

else:
    # Ahora sí existe la columna para poder particionar
    result_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("create_date") \
        .saveAsTable("movie_gold.results_country_prod_company")

print(f"¡Carga de {v_file_date} finalizada!")






# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT create_date, COUNT(1) as total_registros
# MAGIC FROM movie_gold.results_country_prod_company
# MAGIC GROUP BY create_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_gold.results_country_prod_company;