# Databricks notebook source
#Utilizando el mismo ejemplo utilizado en el capítulo para guardar en parquet y guardar los datos en los formatos:
#i. JSON
#ii. CSV (dándole otro nombre para evitar sobrescribir el fichero origen)
#iii. AVRO


# COMMAND ----------

#use json
df4 = spark.read.format("json").load("/databricks-datasets/learning-spark-v2/flights/summary-data/json/*")

display(df4)

# COMMAND ----------

#use csv
df3 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("mode", "PERMISSIVE").load("/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*")

display(df3)

# COMMAND ----------

#use avro

df4 = spark.read.format("AVRO").load("/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*")

display(df4)


# COMMAND ----------

