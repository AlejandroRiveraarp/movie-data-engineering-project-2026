![Azure](https://img.shields.io/badge/Azure-DataFactory-blue)
![Databricks](https://img.shields.io/badge/Databricks-Spark-orange)
![Python](https://img.shields.io/badge/Python-3.10-yellow)

Movie Data Engineering Pipeline
![Intro](movie-history/imagenes/1.png)

## Arquitectura del Proyecto

El pipeline sigue el siguiente flujo:

* **Bronze**: Raw data ingestion from Azure Data Lake (CSV, JSON)
* **Silver**: Data cleaning, transformation, and enrichment
* **Gold**: Business-level aggregations and analytics-ready tables
* **Analysis**: SQL-based insights and reporting

📌 Diagrama de la arquitectura del proyecto:

![Arquitectura](movie-history/imagenes/2.png)

The pipeline follows a layered architecture:



---
## 📥 Ingesta de Datos (Bronze Layer)

Los datos son ingeridos desde archivos hacia el Data Lake utilizando notebooks en Databricks.

* Uso de parámetros (`widgets`)
* Lectura de archivos JSON/CSV
* Escritura en formato Delta

📌 Notebook de ingestión en Databricks:

![Ingesta](movie-history/imagenes/3.png)

---
## ⚙️ Transformación de Datos (Silver Layer)

En esta capa se realiza:

* Limpieza de datos
* Normalización de columnas
* Joins entre datasets (movies, languages, genres, etc.)

📌 Proceso de transformación y joins:

![Transformación](movie-history/imagenes/4.png)

---
## 🥇 Capa Gold (Data Warehouse)

Se generan tablas agregadas optimizadas para análisis:

* Métricas de revenue
* Presupuestos por productora
* Promedios y conteos

📌 Consulta SQL sobre datos en capa Gold:

![Gold](movie-history/imagenes/5.png)

---

## 📊 Análisis de Datos

Ejemplo de análisis: comparación entre presupuesto y revenue por productora.

Este tipo de visualización permite identificar:

* Productoras más rentables
* Relación inversión vs retorno

📌 Visualización de resultados:

![Análisis](movie-history/imagenes/7.png)
![Análisis](movie-history/imagenes/8.png)
![Análisis](movie-history/imagenes/9.png)
![Análisis](movie-history/imagenes/10.png)

---

## ⚙️ Technologies Used

* Azure Data Factory (Orchestration)
* Azure Data Lake Gen2
* Databricks
* PySpark
* Delta Lake
* SQL

---

## 🔄 Data Pipeline Flow

1. Ingest raw data into Bronze layer
2. Transform and clean data into Silver layer
3. Build aggregated datasets in Gold layer
4. Perform analytical queries in Analysis layer

---

## 📊 Example Analysis

Top countries by revenue between 2010 and 2015:

* Total number of movies
* Total and average budget
* Total and average revenue

---

## 📁 Project Structure

```
movie-history/
 ├── notebooks/
 │   ├── bronze/
 │   ├── silver/
 │   ├── gold/
 │   ├── analysis/
 │   └── includes/
```

---
![Análisis](movie-history/imagenes/11.png)


## 🔐 Security Note

Sensitive configurations such as storage account keys are not included in this repository.

---

## 📌 Author

Developed as a personal Data Engineering project using Azure ecosystem tools.
