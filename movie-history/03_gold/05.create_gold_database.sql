-- Databricks notebook source
-- MAGIC %run ../ingestion/00_config_adls

-- COMMAND ----------

SHOW STORAGE CREDENTIALS;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS databricks_course_ws2.movie_gold;

-- COMMAND ----------

DESC DATABASE movie_gold 