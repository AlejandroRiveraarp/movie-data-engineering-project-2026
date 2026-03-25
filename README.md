# 🎬 Movie Data Engineering Pipeline

## 🚀 Project Overview

End-to-end data engineering pipeline built using Azure Data Factory and Databricks, following the Medallion Architecture (Bronze, Silver, Gold).

This project processes movie datasets from raw ingestion to business-ready analytics.

---

## 🏗️ Architecture

The pipeline follows a layered architecture:

* **Bronze**: Raw data ingestion from Azure Data Lake (CSV, JSON)
* **Silver**: Data cleaning, transformation, and enrichment
* **Gold**: Business-level aggregations and analytics-ready tables
* **Analysis**: SQL-based insights and reporting

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

## 🔐 Security Note

Sensitive configurations such as storage account keys are not included in this repository.

---

## 📌 Author

Developed as a personal Data Engineering project using Azure ecosystem tools.
