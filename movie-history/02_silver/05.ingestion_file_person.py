# Databricks notebook source
spark.read.option("header", True).csv("abfss://bronce@prueba1db.dfs.core.windows.net/2024-12-16/person.json").createOrReplaceTempView("v_person_1")

# COMMAND ----------

# MAGIC %sql SELECT COUNT(1) FROM v_person_1 

# COMMAND ----------

# MAGIC %run ./00_config_adls

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("p_enviroment", "")
dbutils.widgets.get("p_enviroment")
v_enviroment = dbutils.widgets.get("p_enviroment")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2024-12-16")

v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Paso 1 - Leer el archivo JSON utilizando "DataFrameReader" de spark 

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType, IntegerType

# COMMAND ----------

name_schema = StructType(fields = [
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])
    

# COMMAND ----------

persons_schema = StructType(fields = [
    StructField("personId", IntegerType(), False),
    StructField("personName", name_schema)
])

display(persons_schema)

# COMMAND ----------

person_df = spark.read \
    .schema(persons_schema) \
    .json(f"{bronze_folder_path}/{v_file_date}/person.json")
display(person_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Paso 2 - Renombrar columnas y añadir nuevas columnas 
# MAGIC 1 - "personID" renombrar a "person_Id" 
# MAGIC
# MAGIC 2 - Agregar columnas "ingestion_date" y "enviroment"
# MAGIC
# MAGIC 3 - Agregar la columna "name" a partir de la concatenacion de "forename" y "surname"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit,concat

# COMMAND ----------

person_whit_colum = person_df \
    .withColumnRenamed("personId", "person_id") \
    .withColumn("name",\
         concat(col("personName.forename"),\
         lit(""), col("personName.surname"))
    )
display(person_whit_colum)

       

# COMMAND ----------

person_whit_colum= add_ingestion_Date(person_whit_colum)\
    .withColumn("enviroment", lit(v_enviroment))\
        .withColumn("file_date", lit(v_file_date))
display(person_whit_colum)

# COMMAND ----------

display(person_whit_colum)


# COMMAND ----------

person_df.printSchema( )

# COMMAND ----------

# MAGIC %md
# MAGIC ####Paso 3 - Elimianr columnas no requeridas 

# COMMAND ----------

person_final_df = person_whit_colum.drop(col("personName"
))

display(person_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Paso 4 - Escribir salida en formato "parket"

# COMMAND ----------

#overwrite_partitions("databricks_course_ws2.movie_silver","person","file_date",v_file_date)

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists("movie_silver.person"):
    

    deltaTable = DeltaTable.forName(spark, 'movie_silver.person')

    deltaTable.alias('tgt') \
    .merge(
        person_final_df.alias('src'),
        'tgt.person_id = src.person_id AND tgt.file_date = src.file_date'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
    



else:
    person_final_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("file_date") \
        .saveAsTable("movie_silver.person")





# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT file_date, COUNT(1)
# MAGIC FROM movie_silver.person
# MAGIC GROUP BY file_date;

# COMMAND ----------

