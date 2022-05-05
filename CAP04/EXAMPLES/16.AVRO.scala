// Databricks notebook source
// DBTITLE 1,Reading an Avro file into a dataframe
//Reading an Avro file into a DataFrame using DataFrameReader is consistent in usage with the other data sources we have discussed in this section:
val df = spark.read.format("avro")
.load("/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*")
display(df)

// COMMAND ----------

// DBTITLE 1,Reading an avro file into a spark sql table
// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW episode_tbl
// MAGIC  USING avro
// MAGIC  OPTIONS (
// MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"
// MAGIC  )

// COMMAND ----------

//Once you’ve created a table, you can read data into a DataFrame using SQL:
display(spark.sql("SELECT * FROM episode_tbl"))

// COMMAND ----------

// DBTITLE 1,Writing dataframes to avro files
//Writing a DataFrame as an Avro file is simple. As usual, specify the appropriate DataFrameWriter methods and arguments, and supply the location to save the Avro files to
df.write
 .format("avro")
 .mode("overwrite")
 .save("/tmp/data/avro/df_avro")

// COMMAND ----------

