// Databricks notebook source
/*Writing a DataFrame to a SQL table is as easy as writing to a file—just use saveAsTable() instead of save(). This will create a managed table called us_delay_flights_tbl:*/
val file = """/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet/"""
val df = spark.read.format("parquet").load(file)

df.write
 .mode("overwrite")
 .saveAsTable("us_delay_flights_tbl")



// COMMAND ----------

