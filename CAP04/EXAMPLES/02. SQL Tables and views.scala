// Databricks notebook source
/*we’ll create a database called learn_spark_db and tell Spark we want to use that database:*/
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")


// COMMAND ----------

/*To create a managed table within the database learn_spark_db, you can issue a SQL query like the following*/
spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")



// COMMAND ----------

// MAGIC %python
// MAGIC #You can do the same thing using the DataFrame API like this:
// MAGIC #csv_file = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
// MAGIC # Schema as defined in the preceding example
// MAGIC #schema="date STRING, delay INT, distance INT, origin STRING, #destination STRING"
// MAGIC #flights_df = spark.read.csv(csv_file, schema=schema)
// MAGIC #flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

// COMMAND ----------

//creating an unmanaged table:
spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT, 
 distance INT, origin STRING, destination STRING) 
 USING csv OPTIONS (PATH 
 '/databricks-datasets/learning-spark-v2/flights/departuredelays.csv')""")

// COMMAND ----------

