// Databricks notebook source
//Create managed and unmanaged tables using Spark SQL and the DataFrame API.

//managed table
spark.sql("create or replace table ex1_tbl (nombre STRING, apellidos String, edad INT, fecha_nacimiento DATE)")

//unmanaged

spark.sql("""CREATE TABLE ex1V2_tbl(date STRING, delay INT, distance INT, origin STRING, destination STRING) 
 USING csv OPTIONS (PATH '/databricks-datasets/learning-spark-v2/flights/departuredelays.csv')""")



// COMMAND ----------

