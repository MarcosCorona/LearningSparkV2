// Databricks notebook source
// DBTITLE 1,Reading an ORC file into a DataFrame
//To read in a DataFrame using the ORC vectorized reader, you can just use the normal DataFrameReader methods and options
val file = "/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"

val df = spark.read.format("orc").load(file)

display(df.take(10))

// COMMAND ----------

// DBTITLE 1,Reading an ORC le into a Spark SQL table
// MAGIC %sql
// MAGIC --there is no difference from parquet, JSON, CSV or avro when creating a sql view using an orc data source
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
// MAGIC  USING orc
// MAGIC  OPTIONS (
// MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"
// MAGIC  )

// COMMAND ----------

//Once a table is created, you can read data into a DataFrame using SQL as usual
display(spark.sql("Select * from us_delay_flights_tbl"))

// COMMAND ----------

// DBTITLE 1,Writing dataframes to ORC files
df.write.format("orc")
.mode("overwrite")
.option("compression", "snappy")
.save("/tmp/data/orc/df_orc")

// COMMAND ----------

display(df)

// COMMAND ----------

