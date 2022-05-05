// Databricks notebook source
//Read from and write to various built-in data sources and file formats

val df4 = spark.read.format("csv")
 .load("/FileStore/tables/mnm_dataset.csv")


// COMMAND ----------

display(df4)

// COMMAND ----------

df4.write.format("json")
 .mode("overwrite")
 .save("/FileStore/tables/mnm_dataset.json")


// COMMAND ----------

