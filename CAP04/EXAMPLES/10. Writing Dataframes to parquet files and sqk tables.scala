// Databricks notebook source
//dataframe used:
val file = """/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet"""

val df = spark.read.format("parquet").load(file)

// COMMAND ----------

/*. To write a DataFrame you simply use the methods and arguments to the DataFrame Writer outlined earlier in this chapter, supplying the location to save the Parquet files
to.*/
df.write.format("parquet")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/tmp/data/parquet/df_parquet")


// COMMAND ----------

/*Writing a DataFrame to a SQL table is as easy as writing to a file—just use saveAsTable() instead of save(). This will create a managed table called us_delay_flights_tbl:*/
df.write
 .mode("overwrite")
 .saveAsTable("us_delay_flights_tbl")


// COMMAND ----------

spark.sql("use learn_spark_db")
spark.sql("select * from us_delay_flights_tbl")

// COMMAND ----------

