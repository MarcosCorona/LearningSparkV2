// Databricks notebook source
/*While we won’t comprehensively enumerate all the different combinations of arguments and options, the documentation for Python, Scala, R, and Java offers suggestions and guidance. It’s worthwhile to show a couple of examples, though*/
// Use Parquet 
val file = """/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet"""

val df = spark.read.format("parquet").load(file)



// COMMAND ----------

// Use CSV
val df3 = spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .option("mode", "PERMISSIVE")
 .load("/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*")



// COMMAND ----------

// Use JSON
val df4 = spark.read.format("json")
 .load("/databricks-datasets/learning-spark-v2/flights/summary-data/json/*")


// COMMAND ----------

display(df4)

// COMMAND ----------

