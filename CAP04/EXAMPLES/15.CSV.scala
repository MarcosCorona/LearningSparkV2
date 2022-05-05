// Databricks notebook source
// DBTITLE 1,Reading csv into dataframes

//As with the other built-in data sources, you can use the DataFrameReader methods and arguments to read a CSV file into a DataFrame:
val file = "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*"
val schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"
val df = spark.read.format("csv")
 .schema(schema)
 .option("header", "true")
 .option("mode", "FAILFAST") // Exit if any errors
 .option("nullValue", "") // Replace any null data with quotes
 .load(file)

// COMMAND ----------

display(df)

// COMMAND ----------

// DBTITLE 1,Reading a csv file into a spark sql table
// MAGIC %sql
// MAGIC --Creating a SQL table from a CSV data source is no different from using Parquet or JSON:
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
// MAGIC  USING csv
// MAGIC  OPTIONS (
// MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*",
// MAGIC  header "true",
// MAGIC  inferSchema "true",
// MAGIC  mode "FAILFAST"
// MAGIC  )

// COMMAND ----------

display(spark.sql("select * from us_delay_flights_tbl"))

// COMMAND ----------

// DBTITLE 1,Writing dataframes to CSV files
//Specify the appropriate DataFrameWriter methods and arguments, and supply the location to save the CSV files to:
df.write.format("csv")
.mode("overwrite")
.save("/tmp/data/csv/df_csv")

// COMMAND ----------

val file = "/tmp/data/csv/df_csv"
val df = spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .option("mode", "FAILFAST") // Exit if any errors
 .option("nullValue", "") // Replace any null data with quotes
 .load(file)

// COMMAND ----------

display(df)