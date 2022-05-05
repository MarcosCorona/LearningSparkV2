-- Databricks notebook source
/* Leer el CSV del ejemplo del cap2 y obtener la estructura del schema dado por 
defecto*/
SELECT * FROM mnm_table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Leer el CSV del ejemplo del cap2 y obtener la estructura del schema dado por defecto
-- MAGIC 
-- MAGIC mnmDF = spark.table("mnm_table")
-- MAGIC display(mnmDF.select("*"))

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC val mnmdf = spark.table("mnm_table")
-- MAGIC display(mnmdf.select("*"))

-- COMMAND ----------

