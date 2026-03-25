# Databricks notebook source

# COMMAND ----------
# 1. Crear schema
spark.sql("""
CREATE SCHEMA IF NOT EXISTS movie_bronze;
""")

# COMMAND ----------
# 2. Seleccionar catálogo y schema
spark.sql("""
USE CATALOG databricks_course_ws2;
USE SCHEMA movie_bronze;
""")

# COMMAND ----------
# 3. Crear volumen
spark.sql("""
CREATE VOLUME IF NOT EXISTS bronze_volume;
""")

# COMMAND ----------
# 4. Listar archivos
spark.sql("""
LIST '/Volumes/databricks_course_ws2/movie_bronze/bronze_volume/';
""")

# COMMAND ----------
# 5. Verificar contexto
spark.sql("""
SELECT current_catalog(), current_schema();
""")

# COMMAND ----------
# 6. Limpiar tabla movies
spark.sql("""
DROP TABLE IF EXISTS databricks_course_ws2.movie_bronze.movies;
""")

# COMMAND ----------
# 7. Crear tabla movies (DELTA)
spark.sql("""
CREATE OR REPLACE TABLE databricks_course_ws2.movie_bronze.movies
USING DELTA
AS
SELECT
  movieId,
  title,
  budget,
  homePage,
  overview,
  popularity,
  releaseDate,
  revenue,
  durationTime,
  movieStatus,
  tagline,
  voteAverage,
  voteCount
FROM read_files(
  '/Volumes/databricks_course_ws2/movie_bronze/bronze_volume/movie.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);
""")

# COMMAND ----------
spark.sql("""
SELECT * FROM databricks_course_ws2.movie_bronze.movies LIMIT 5;
""")

# COMMAND ----------
# 8. Verificar formato
spark.sql("""
DESCRIBE DETAIL databricks_course_ws2.movie_bronze.movies;
""")

# COMMAND ----------
# LANGUAGE
spark.sql("""
DROP TABLE IF EXISTS databricks_course_ws2.movie_bronze.language;

CREATE TABLE databricks_course_ws2.movie_bronze.language
USING DELTA
AS
SELECT 
  CAST(languageId AS INT) AS languageId,
  CAST(languageName AS STRING) AS languageName
FROM read_files(
  '/Volumes/databricks_course_ws2/movie_bronze/bronze_volume/language.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);
""")

# COMMAND ----------
# GENRE
spark.sql("""
DROP TABLE IF EXISTS databricks_course_ws2.movie_bronze.genre;

CREATE TABLE databricks_course_ws2.movie_bronze.genre
USING DELTA
AS
SELECT 
  CAST(genreId AS INT) AS genreId,
  CAST(genreName AS STRING) AS genreName
FROM read_files(
  '/Volumes/databricks_course_ws2/movie_bronze/bronze_volume/genre.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);
""")

# COMMAND ----------
# COUNTRIES
spark.sql("""
DROP TABLE IF EXISTS databricks_course_ws2.movie_bronze.countries;

CREATE TABLE databricks_course_ws2.movie_bronze.countries
USING DELTA
AS
SELECT
  countryId,
  countryIsoCode,
  countryName
FROM read_files(
  '/Volumes/databricks_course_ws2/movie_bronze/bronze_volume/country.json',
  format => 'json',
  inferSchema => true
);
""")

# COMMAND ----------
# PERSON
spark.sql("""
CREATE TABLE IF NOT EXISTS databricks_course_ws2.movie_bronze.person
USING DELTA
AS
SELECT
  personId,
  personName.forename AS forename,
  personName.surname AS surname
FROM read_files(
  '/Volumes/databricks_course_ws2/movie_bronze/bronze_volume/person.json',
  format => 'json',
  inferSchema => true
);
""")

# COMMAND ----------
# MOVIE GENRE
spark.sql("""
DROP TABLE IF EXISTS databricks_course_ws2.movie_bronze.movie_genre;

CREATE TABLE databricks_course_ws2.movie_bronze.movie_genre
USING DELTA
AS
SELECT
  movieId,
  genreId
FROM read_files(
  '/Volumes/databricks_course_ws2/movie_bronze/bronze_volume/movie_genre.json',
  format => 'json',
  inferSchema => true
);
""")

# COMMAND ----------
# MOVIE CAST
spark.sql("""
DROP TABLE IF EXISTS databricks_course_ws2.movie_bronze.movie_cast;

CREATE TABLE databricks_course_ws2.movie_bronze.movie_cast
USING DELTA
AS
SELECT
  movieId,
  personId,
  characterName,
  genderId,
  castOrder
FROM read_files(
  '/Volumes/databricks_course_ws2/movie_bronze/bronze_volume/movie_cast.json',
  format => 'json',
  multiLine => true,
  inferSchema => true
);
""")

# COMMAND ----------
# LANGUAGE ROLE
spark.sql("""
DROP TABLE IF EXISTS databricks_course_ws2.movie_bronze.language_role;

CREATE TABLE databricks_course_ws2.movie_bronze.language_role
USING DELTA
AS
SELECT
  roleId,
  languageRole
FROM read_files(
  '/Volumes/databricks_course_ws2/movie_bronze/bronze_volume/language_role.json',
  format => 'json',
  multiLine => true,
  inferSchema => true
);
""")

# COMMAND ----------
# PRODUCTION COMPANY (ADLS)
spark.sql("""
CREATE OR REPLACE TABLE databricks_course_ws2.movie_bronze.production_company
USING DELTA
AS
SELECT
  CAST(_c0 AS INT) AS company_id,
  CAST(_c1 AS STRING) AS company_name
FROM read_files(
  'abfss://bronce@prueba1db.dfs.core.windows.net/production_company',
  format => 'CSV',
  header => false,
  inferSchema => false
);
""")

# COMMAND ----------
# MOVIE COMPANY
spark.sql("""
CREATE OR REPLACE TABLE databricks_course_ws2.movie_bronze.movie_company
USING DELTA
AS
SELECT
  CAST(_c0 AS INT) AS movie_id,
  CAST(_c1 AS INT) AS company_id
FROM read_files(
  'abfss://bronce@prueba1db.dfs.core.windows.net/movie_company',
  format => 'CSV',
  header => false,
  inferSchema => false
);
""")

# COMMAND ----------
# MOVIE LANGUAGES
spark.sql("""
CREATE OR REPLACE TABLE databricks_course_ws2.movie_bronze.movie_languages
USING DELTA
AS
SELECT
  CAST(movieId AS INT) AS movie_id,
  CAST(languageId AS INT) AS language_id,
  CAST(languageRoleId AS INT) AS language_role_id
FROM read_files(
  'abfss://bronce@prueba1db.dfs.core.windows.net/movie_language',
  format => 'JSON',
  multiLine => true
);
""")

# COMMAND ----------
# PRODUCTION COUNTRY
spark.sql("""
CREATE OR REPLACE TABLE databricks_course_ws2.movie_bronze.production_country
USING DELTA
AS
SELECT
  CAST(movieId AS INT) AS movieId,
  CAST(countryId AS INT) AS countryId
FROM read_files(
  'abfss://bronce@prueba1db.dfs.core.windows.net/production_country',
  format => 'JSON',
  multiLine => true
);
""")
