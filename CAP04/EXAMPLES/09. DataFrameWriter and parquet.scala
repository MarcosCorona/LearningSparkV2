// Databricks notebook source
//uses of arguments of write.
val file = """/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet"""

val df = spark.read.format("parquet").load(file)
// Use JSON
val location = "/FileStore/shared.data/"
df.write.format("json").mode("overwrite").save(location)

// COMMAND ----------

/*To read Parquet files into a DataFrame, you simply specify the format and path:*/
val file = """/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet/"""
val df = spark.read.format("parquet").load(file)

// COMMAND ----------

// MAGIC %sql
// MAGIC /*Reading Parquet files into a Spark SQL table*/
// MAGIC /*As well as reading Parquet files into a Spark DataFrame, you can also create a Spark
// MAGIC SQL unmanaged table or view directly using SQL:*/
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
// MAGIC  USING parquet
// MAGIC  OPTIONS (path "/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet/" )

// COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show()

// COMMAND ----------

