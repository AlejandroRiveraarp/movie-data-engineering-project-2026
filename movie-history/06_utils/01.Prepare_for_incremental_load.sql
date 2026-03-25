-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Eliminar y volver a crear todas las bases de datos de Datos

-- COMMAND ----------

DROP SCHEMA IF EXISTS databricks_course_ws2.movie_silver CASCADE;

-- COMMAND ----------

CREATE SCHEMA databricks_course_ws2.movie_silver
MANAGED LOCATION 'abfss://silver@prueba1db.dfs.core.windows.net/';

-- COMMAND ----------

DROP SCHEMA IF EXISTS databricks_course_ws2.movie_gold CASCADE;

-- COMMAND ----------

-- Creamos el esquema Gold apuntando a su contenedor
CREATE SCHEMA IF NOT EXISTS databricks_course_ws2.movie_gold
MANAGED LOCATION 'abfss://gold@prueba1db.dfs.core.windows.net/';