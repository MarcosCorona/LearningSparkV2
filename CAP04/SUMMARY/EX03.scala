// Databricks notebook source
//Employ the spark.sql programmatic interface to issue SQL queries on structured data stored as Spark SQL tables or views.
display(spark.sql("Select * from us_delay_flights_tbl1"))

// COMMAND ----------

