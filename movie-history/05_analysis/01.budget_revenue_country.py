-- Databricks notebook source
USE movie_gold;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN movie_gold;

-- COMMAND ----------

SELECT * FROM result_group_movie_country;

-- COMMAND ----------

SELECT country_name,
       COUNT(country_name) AS total_country, -- Esto contará las filas por país/año
       SUM(total_budget) AS suma_total_presupuesto,
       CAST(AVG(total_budget) AS DECIMAL (18,2)) AS promedio_presupuesto,
       SUM(total_revenue) AS suma_total_ganancia,
       CAST(AVG(total_revenue) AS DECIMAL (18,2)) AS promedio_ganancia
FROM movie_gold.result_group_movie_country
WHERE year_release_date BETWEEN 2010 AND 2015
GROUP BY country_name
ORDER BY suma_total_ganancia DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### codigo anterior a continuacion

-- COMMAND ----------

-- MAGIC %md
-- MAGIC SELECT country_name,
-- MAGIC        COUNT(country_name) AS total_country,
-- MAGIC        SUM(budget) AS total_budget,
-- MAGIC        CAST(AVG(budget) AS DECIMAL (18,2)) AS avg_budget,
-- MAGIC        SUM(revenue) AS total_revenue,
-- MAGIC        CAST(AVG(revenue) AS DECIMAL (18,2)) AS avg_revenue
-- MAGIC FROM results_movie
-- MAGIC WHERE year_release_date BETWEEN 2010 AND 2015
-- MAGIC GROUP BY country_name
-- MAGIC ORDER BY total_revenue DESC;

-- COMMAND ----------

