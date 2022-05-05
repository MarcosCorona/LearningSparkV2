# Databricks notebook source
# MAGIC %scala
# MAGIC /*
# MAGIC Revisar al guardar los ficheros (p.e. json, csv, etc) el número de ficheros 
# MAGIC creados, revisar su contenido para comprender (constatar) como se guardan.
# MAGIC i. ¿A qué se debe que hayan más de un fichero?
# MAGIC ii. ¿Cómo obtener el número de particiones de un DataFrame?
# MAGIC iii. ¿Qué formas existen para modificar el número de particiones de un 
# MAGIC DataFrame?
# MAGIC iv. Llevar a cabo el ejemplo modificando el número de particiones a 1 y 
# MAGIC revisar de nuevo el/los ficheros guardados.*/

# COMMAND ----------

#i. ¿A qué se debe que hayan más de un fichero?

#Tratando como ejemplo el csv nos crea loq que serían 4 archivos o jobs

#1º escanea texto e implanta lo siguiente :
#Whole-Stage Java Code Generation improves the execution performance of a query by collapsing a query tree into a single optimized function that eliminates virtual function calls and leverages CPU registers for intermediate data.

#en resumen mejora la ejecución juntando un arbol de consultas en una sola optimizada que elimina las llamadas a funciones virtuales y aprovecha los registros de CPU para datos intermedios.

#2º lo reescanea y lo deserializa a objeto, posteriormente lo particiona en maps

#3º escanea el csv

# COMMAND ----------

#¿Cómo obtener el número de particiones de un DataFrame?
df3 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("mode", "PERMISSIVE").load("/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*")

#hay que convertirlo en rdd primero
df3.rdd.getNumPartitions



# COMMAND ----------

