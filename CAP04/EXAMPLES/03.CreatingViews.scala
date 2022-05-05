// Databricks notebook source
// MAGIC %sql 
// MAGIC 
// MAGIC use learn_spark_db

// COMMAND ----------

// MAGIC %sql
// MAGIC /*create global temporary and temporary views consisting of just that slice of the table:*/
// MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
// MAGIC  SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE
// MAGIC  origin = 'SFO';

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS
// MAGIC  SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE
// MAGIC  origin = 'JFK'

// COMMAND ----------

// MAGIC %python 
// MAGIC #You can accomplish the same thing with the DataFrame API as follows:
// MAGIC df_sfo = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")
// MAGIC df_jfk = spark.sql("SELECT date, delay, origin, destination FROM  us_delay_flights_tbl WHERE origin = 'JFK'")

// COMMAND ----------

// MAGIC %python
// MAGIC # Create a temporary and global temporary view
// MAGIC df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
// MAGIC df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

// COMMAND ----------

// MAGIC %sql
// MAGIC /*, you can issue queries against them just as you would
// MAGIC against a table. Keep in mind that when accessing a global temporary view you must use the prefix global_temp.<view_name>*/
// MAGIC SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view

// COMMAND ----------

// MAGIC %sql
// MAGIC /*By contrast, you can access the normal temporary view without the global_temp prefix*/
// MAGIC SELECT * FROM us_origin_airport_JFK_tmp_view

// COMMAND ----------

// MAGIC %sql
// MAGIC /*You can also drop a view just like you would a table:*/
// MAGIC DROP VIEW IF EXISTS us_origin_airport_SFO_global_tmp_view;
// MAGIC DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view

// COMMAND ----------

