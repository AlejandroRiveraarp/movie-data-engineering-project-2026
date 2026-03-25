-- Databricks notebook source
DELETE FROM movie_gold.results_movie;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.widgets.text("p_file_date", "2024-12-30")
-- MAGIC
-- MAGIC v_file_date = dbutils.widgets.get("p_file_date")
-- MAGIC print(f"La fecha capturada es: '{v_file_date}'")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("""
-- MAGIC CREATE TABLE IF NOT EXISTS movie_gold.results_movie
-- MAGIC (
-- MAGIC     year_release_date INT,
-- MAGIC     country_name STRING,
-- MAGIC     company_name STRING,
-- MAGIC     budget FLOAT,
-- MAGIC     revenue FLOAT,
-- MAGIC     movie_id INT,
-- MAGIC     country_id INT,
-- MAGIC     company_id INT,
-- MAGIC     created_date DATE,
-- MAGIC     updated_date DATE
-- MAGIC )
-- MAGIC USING DELTA
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Lista de tablas a limpiar
-- MAGIC tablas_a_limpiar = [
-- MAGIC     "movie_silver.production_country",
-- MAGIC     "movie_silver.movie_company",
-- MAGIC     "movie_silver.country",  # Verifica si es 'country' o 'countries' según tu esquema
-- MAGIC     "movie_silver.production_company"
-- MAGIC ]
-- MAGIC
-- MAGIC for tabla in tablas_a_limpiar:
-- MAGIC     try:
-- MAGIC         # Leemos, quitamos duplicados y sobreescribimos
-- MAGIC         df_limpio = spark.read.table(tabla).dropDuplicates()
-- MAGIC         df_limpio.write.format("delta").mode("overwrite").saveAsTable(tabla)
-- MAGIC         print(f"✅ Tabla {tabla} limpiada con éxito.")
-- MAGIC     except Exception as e:
-- MAGIC         print(f"⚠️ No se pudo limpiar {tabla}: {e}")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC CREATE OR REPLACE TEMP VIEW v_result_movie AS
-- MAGIC SELECT DISTINCT
-- MAGIC   M.year_release_date, 
-- MAGIC   C.country_name,
-- MAGIC   PCO.company_name,
-- MAGIC   M.budget,
-- MAGIC   M.revenue,
-- MAGIC   M.movie_id,
-- MAGIC   C.country_id,
-- MAGIC   PCO.company_id
-- MAGIC FROM movie_silver.movies M
-- MAGIC INNER JOIN movie_silver.production_country PC ON M.movie_id = PC.movie_id
-- MAGIC INNER JOIN movie_silver.country C ON PC.country_id = C.country_id
-- MAGIC INNER JOIN movie_silver.movie_company MC ON M.movie_id = MC.movie_id
-- MAGIC INNER JOIN movie_silver.production_company PCO ON MC.company_id = PCO.company_id
-- MAGIC WHERE M.file_date = '{v_file_date}'
-- MAGIC """)
-- MAGIC
-- MAGIC # Verificación del conteo
-- MAGIC conteo = spark.table("v_result_movie").count()
-- MAGIC print(f"✅ Conteo de la vista: {conteo} registros")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC v_result_movie_clean = spark.table("v_result_movie").dropDuplicates()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC     MERGE INTO movie_gold.results_movie tgt
-- MAGIC     USING v_result_movie src
-- MAGIC     ON (
-- MAGIC         tgt.movie_id = src.movie_id AND 
-- MAGIC         tgt.country_id = src.country_id AND 
-- MAGIC         tgt.company_id = src.company_id
-- MAGIC     )
-- MAGIC     WHEN MATCHED THEN
-- MAGIC       UPDATE SET
-- MAGIC         tgt.year_release_date = src.year_release_date,
-- MAGIC         tgt.country_name = src.country_name,
-- MAGIC         tgt.company_name = src.company_name,
-- MAGIC         tgt.budget = src.budget,
-- MAGIC         tgt.revenue = src.revenue,
-- MAGIC         tgt.updated_date = current_timestamp()
-- MAGIC     WHEN NOT MATCHED THEN
-- MAGIC       INSERT (
-- MAGIC         year_release_date, 
-- MAGIC         country_name, 
-- MAGIC         company_name, 
-- MAGIC         budget, 
-- MAGIC         revenue, 
-- MAGIC         movie_id, 
-- MAGIC         country_id, 
-- MAGIC         company_id, 
-- MAGIC         created_date
-- MAGIC       )
-- MAGIC       VALUES (
-- MAGIC         src.year_release_date, 
-- MAGIC         src.country_name, 
-- MAGIC         src.company_name, 
-- MAGIC         src.budget, 
-- MAGIC         src.revenue, 
-- MAGIC         src.movie_id, 
-- MAGIC         src.country_id, 
-- MAGIC         src.company_id, 
-- MAGIC         current_timestamp()
-- MAGIC       )
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC total_filas = spark.table("v_result_movie").count()
-- MAGIC filas_unicas = spark.table("v_result_movie").dropDuplicates().count()
-- MAGIC
-- MAGIC print(f"Total de filas: {total_filas}")
-- MAGIC print(f"Filas sin duplicados: {filas_unicas}")
-- MAGIC print(f"Diferencia (filas repetidas): {total_filas - filas_unicas}")

-- COMMAND ----------

SELECT * FROM movie_gold.results_movie

-- COMMAND ----------

SELECT * FROM v_result_movie;