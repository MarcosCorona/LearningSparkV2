-- Databricks notebook source
use learn_spark_db

-- COMMAND ----------

/*you can specify a table as LAZY, meaning
that it should only be cached when it is first used instead of immediately:*/
CACHE LAZY TABLE us_delay_flights_tbl


-- COMMAND ----------

UNCACHE TABLE us_delay_flights_tbl

-- COMMAND ----------

