// Databricks notebook source
/*Instead of reading from an external JSON file,
you can simply use SQL to query the table and assign the returned result to a DataFrame: */
spark.sql("USE learn_spark_db")
val usFlightsDF = spark.sql("SELECT * FROM us_delay_flights_tbl")
val usFlightsDF2 = spark.table("us_delay_flights_tbl")

// COMMAND ----------

display(usFlightsDF2)

// COMMAND ----------

