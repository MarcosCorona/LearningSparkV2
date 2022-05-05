// Databricks notebook source
// MAGIC %python
// MAGIC #Reading a dataset into a temporaryview
// MAGIC from pyspark.sql import SparkSession 
// MAGIC # Create a SparkSession
// MAGIC spark = (SparkSession
// MAGIC  .builder
// MAGIC  .appName("SparkSQLExampleApp")
// MAGIC  .getOrCreate())
// MAGIC # Path to data set
// MAGIC csv_file = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
// MAGIC # Read and create a temporary view
// MAGIC # Infer schema (note that for larger files you 
// MAGIC # may want to specify the schema)
// MAGIC df = (spark.read.format("csv")
// MAGIC  .option("inferSchema", "true")
// MAGIC  .option("header", "true")
// MAGIC  .load(csv_file))
// MAGIC          
// MAGIC df.createOrReplaceTempView("us_delay_flights_tbl")
// MAGIC          

// COMMAND ----------

// MAGIC %python
// MAGIC #First, we’ll find all flights whose distance is greater than 1,000 miles:
// MAGIC display(spark.sql("select distance, origin, destination from us_delay_flights_tbl where distance > 1000"))

// COMMAND ----------

// MAGIC %python
// MAGIC #As the results show, all of the longest flights were between Honolulu (HNL) and NewYork (JFK). Next, we’ll find all flights between San Francisco (SFO) and Chicago(ORD) with at least a two-hour delay:
// MAGIC 
// MAGIC display(spark.sql("Select delay, origin, destination from us_delay_flights_tbl where delay > 120 and origin = 'SFO' and destination = 'ORD'"))

// COMMAND ----------

// MAGIC %python
// MAGIC #Let’s try a more complicated query where we use the CASE clause in SQL. In the following example, we want to label all US flights, regardless of origin and destination, with an indication of the delays they experienced: Very Long Delays (> 6 hours), Long Delays (2–6 hours), etc. We’ll add these human-readable labels in a new column called Flight_Delays:
// MAGIC display(spark.sql("""Select delay,origin,destination,
// MAGIC             CASE
// MAGIC                 WHEN delay > 360 then 'Very Long delays'
// MAGIC                 WHEN delay > 120 then 'Long delays'
// MAGIC                 WHEN delay > 60 then 'Medium delays'
// MAGIC                 WHEN delay > 0 then 'Low Long delays'
// MAGIC                 when delay = 0 then 'No delays'
// MAGIC             END AS Flight_Delays
// MAGIC             from us_delay_flights_tbl
// MAGIC             order by origin, delay DESC
// MAGIC             """))

// COMMAND ----------

// MAGIC %python
// MAGIC #the first query can be expressed in the Python Data‐Frame API as:
// MAGIC from pyspark.sql.functions import col, desc
// MAGIC (df.select("distance", "origin", "destination")
// MAGIC  .where(col("distance") > 1000)
// MAGIC  .orderBy(desc("distance"))).show(10)

// COMMAND ----------

spark.sql("""SELECT date, delay, origin, destination 
FROM us_delay_flights_tbl 
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' 
ORDER by delay DESC""").show(10)

// COMMAND ----------

//As an exercise, convert the date column into a readable format and findthe days or months when these delays were most common. Were the delays related to winter months or holidays?

//val df1 = spark.read.table("us_delay_flights_tbl")





// COMMAND ----------

// MAGIC %sql 
// MAGIC describe table us_delay_flights_tbl

// COMMAND ----------

//no funciona
df1.withColumn("date", $"date".cast("string"))

df1.write.format("csv")
.mode("overwrite")
.option("overwriteSchema", "true")
.saveAsTable("us_delay_flights_tbl1")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT CAST(CAST( date as string) as DATE) as date from us_delay_flights_tbl;

// COMMAND ----------

