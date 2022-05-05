// Databricks notebook source
// MAGIC %sql
// MAGIC --You can also create a SQL table from a JSON file just like you did with parquet.
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_sql_tbl 
// MAGIC USING json OPTIONS(
// MAGIC path "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
// MAGIC )

// COMMAND ----------

//once is create you can read data into a dataframe using sql:
display(spark.sql("SELECT * FROM us_delay_flights_sql_tbl"))



// COMMAND ----------

