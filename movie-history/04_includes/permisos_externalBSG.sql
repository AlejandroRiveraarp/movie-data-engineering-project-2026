-- Databricks notebook source
SELECT current_user();

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG databricks_course_ws2 TO `databricks-course-german-adf`;
GRANT USE SCHEMA ON SCHEMA databricks_course_ws2.movie_silver TO `databricks-course-german-adf`;
GRANT SELECT, MODIFY ON SCHEMA databricks_course_ws2.movie_silver TO `databricks-course-german-adf`;

-- COMMAND ----------

GRANT READ FILES
ON EXTERNAL LOCATION bronce_external_location
TO `databricks-course-german-adf`;

-- COMMAND ----------

GRANT READ FILES
ON EXTERNAL LOCATION silver_external_location
TO `databricks-course-german-adf`;

-- COMMAND ----------

GRANT WRITE FILES
ON EXTERNAL LOCATION silver_external_location
TO `databricks-course-german-adf`;

-- COMMAND ----------

GRANT WRITE FILES
ON EXTERNAL LOCATION gold_external_location
TO `adatabricks-course-german-adf`;

-- COMMAND ----------

GRANT USE SCHEMA ON SCHEMA databricks_course_ws2.movie_silver TO `databricks-course-german-adf`;
GRANT CREATE TABLE ON SCHEMA databricks_course_ws2.movie_silver TO `databricks-course-german-adf`;

GRANT USE SCHEMA ON SCHEMA databricks_course_ws2.movie_gold TO `databricks-course-german-adf`;
GRANT CREATE TABLE ON SCHEMA databricks_course_ws2.movie_gold TO `databricks-course-german-adf`;

GRANT USE CATALOG ON CATALOG databricks_course_ws2 TO `databricks-course-german-adf`;

-- COMMAND ----------

GRANT SELECT, MODIFY ON TABLE movie_silver.movies TO `databricks-course-german-adf`;

-- COMMAND ----------

GRANT SELECT
ON SCHEMA databricks_course_ws2.movie_silver
TO `databricks-course-german-adf`;

-- COMMAND ----------

GRANT SELECT
ON SCHEMA databricks_course_ws2.movie_gold
TO `databricks-course-german-adf`;