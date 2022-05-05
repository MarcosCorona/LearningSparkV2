// Databricks notebook source
/*after creating the SparkSession variable spark, you can access all the stored metadata through methods like these:*/
spark.sql("USE learn_spark_db")

/* i put it in different cells to see a correcly view of the different catalog uses*/


// COMMAND ----------

display(spark.catalog.listDatabases())


// COMMAND ----------

display(spark.catalog.listTables())


// COMMAND ----------

display(spark.catalog.listColumns("us_delay_flights_tbl"))

// COMMAND ----------

