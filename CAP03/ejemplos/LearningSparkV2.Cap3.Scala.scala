// Databricks notebook source
//Key merits and benefits
//Aggregating all the ages foreach name, group by name,
//and then average the ages.

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.SparkSession

//Creamos un dataframe usando sparksession
val spark = SparkSession
  .builder
  .appName("AuthorsAges")
  .getOrCreate()
//Creamos un dataframe con nombres y edades
val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25), ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")

//Agrupamos por nombres y agregamos sus edades y calculamos la media.

val avgDF = dataDF.groupBy("name").agg(avg("age"))


//RESULTADO:
dataDF.show()
avgDF.show()





// COMMAND ----------

import org.apache.spark.sql.types._
val nameTypes = StringType
val firstName = StringType
val lastName = StringType


// COMMAND ----------

/*WAYS TO DEFINE AN SCHEMA
1 way is define it programmatically, the example will have 3 columns,author,title and pages. */
import org.apache.spark.sql.types._
val schema = StructType(Array(StructField("author", StringType, false),
 StructField("title", StringType, false),
 StructField("pages", IntegerType, false)))

// COMMAND ----------

//easiest way to define the schema:
// In Scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

 val spark = SparkSession
 .builder
 .appName("Example-3_7")
 .getOrCreate()

 // Get the path to the JSON file
 val jsonFile = spark.read
  .option("header","true")
  .option("inferSchema","true")
  .json("/FileStore/shared_uploads/marcos.corona@bosonit.com/blogs.json")
  .map(tuple => tuple.json.replace("\n", "").trim)

 // Define our schema programmatically
 val schema = StructType(Array(StructField("Id", IntegerType, false),
 StructField("First", StringType, false),
 StructField("Last", StringType, false),
 StructField("Url", StringType, false),
 StructField("Published", StringType, false),
 StructField("Hits", IntegerType, false),
 StructField("Campaigns", ArrayType(StringType), false)))
 // Create a DataFrame by reading from the JSON file 
 // with a predefined schema
 val blogsDF = spark.read.schema(schema).json(jsonFile)

 // Show the DataFrame schema as output
 blogsDF.show(false) 
  // Print the schema
 println(blogsDF.printSchema)
 println(blogsDF.schema)



// COMMAND ----------

//Some examples of what we can do with columns in Spark.

//each exampleis followed by its output:
import org.apache.spark.sql.functions._
blogsDF.columns

//access a particular column with col and it returns a Column type
blogsDF.col("Id")

//use an expression to compute a value
blogsDF.select(expr("Hits * 2")).show(2)
//or use col to compute value
blogsDF.select(col("Hits") * 2).show(2)
// Use an expression to compute big hitters for blogs
// This adds a new column, Big Hitters, based on the conditional expression
//blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()
display(blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))))





// COMMAND ----------

//Concatenate three columns, create a new column, and show the 
//newly created concatenated column
blogsDF.withColumn("AuthorsId", (concat(expr("First"), expr("Last"),expr("Id"))))
.select(col("AuthorsId"))
.show(4)

// COMMAND ----------

//these statemens return the same value, showing that
//expr is the same as a col method call
blogsDF.select(expr("Hits")).show(2)
blogsDF.select(col("Hits")).show(2)
blogsDF.select("Hits").show(2)


// COMMAND ----------

//sort by column "Id" in descending order
display(blogsDF.sort(col("Id").desc))

// COMMAND ----------

//Rows are objects in spark and an orderer collection of fields, you can instantiate a row in each of Spark's supported languages and access its fields by an index starting at 0:
import org.apache.spark.sql.Row

// Create a Row
val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015", Array("twitter", "LinkedIn"))

//access using index for individual items
blogRow(1)




// COMMAND ----------

//Row objects can be used to create DataFrames i
val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"),("Marcos Corona", "NV"))
val authorsDF = rows.toDF("Author", "State")
authorsDF.show()

// COMMAND ----------

//sampling a default schema with samplingRatio
val sampleDF = spark
  .read
  .option("samplingRatio", 0.001)
  .option("header", true)
.csv("/FileStore/shared_uploads/marcos.corona@bosonit.com/sf_fire_calls.csv")

// COMMAND ----------

//definiendo el esquema nosotros
val fireSchema = StructType(Array(
 StructField("CallNumber", IntegerType, true),  StructField("UnitID", StringType, true),
 StructField("IncidentNumber", IntegerType, true),
 StructField("CallType", StringType, true), 
 StructField("CallDate", StringType, true), 
 StructField("WatchDate", StringType, true),
 StructField("CallFinalDisposition", StringType, true),
 StructField("AvailableDtTm", StringType, true),
 StructField("Address", StringType, true), 
 StructField("City", StringType, true), 
 StructField("Zipcode", IntegerType, true), 
 StructField("Battalion", StringType, true), 
 StructField("StationArea", StringType, true), 
 StructField("Box", StringType, true), 
 StructField("OriginalPriority", StringType, true), 
 StructField("Priority", StringType, true), 
 StructField("FinalPriority", IntegerType, true), 
 StructField("ALSUnit", BooleanType, true), 
 StructField("CallTypeGroup", StringType, true),
 StructField("NumAlarms", IntegerType, true),
 StructField("UnitType", StringType, true),
 StructField("UnitSequenceInCallDispatch", IntegerType,true),
 StructField("FirePreventionDistrict", StringType, true),
 StructField("SupervisorDistrict", StringType, true),
 StructField("Neighborhood", StringType, true),
 StructField("Location", StringType, true),
 StructField("RowID", StringType, true),
 StructField("Delay", FloatType, true)))

//leemos el fichero csv
val sfFireFile="/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
val fireDF = spark.read.schema(fireSchema)
 .option("header", "true")
 .csv(sfFireFile)

// COMMAND ----------

display(fireDF)

// COMMAND ----------

//saving a dataframe as a parquet file or sql table
val parquetPath = "firedb"
fireDF.write.format("parquet")

// COMMAND ----------

/* In Scala to save as a table 
val parquetTable = "tableFiredb" // name of the table
fireDF.write.format("parquet").saveAsTable(parquetTable)*/


// COMMAND ----------

val fewFireDF = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType").where($"CallType" =!= "Medical Incident")

display(fewFireDF)

// COMMAND ----------

// In Scala
import org.apache.spark.sql.functions._
fireDF
 .select("CallType")
 .where(col("CallType").isNotNull)
 .agg(countDistinct('CallType) as 'DistinctCallTypes)
 .show()


// COMMAND ----------

//diferents uses of distinct call
fireDF
 .select("CallType")
 .where($"CallType".isNotNull)
 .distinct()
 .show(10, false)


// COMMAND ----------

val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMin")
newFireDF
 .select("ResponseDelayedinMin")
 .where($"ResponseDelayedinMin" > 5)
 .show(5, false)


// COMMAND ----------

//conversion of dates
val fireTsDF = newFireDF
 .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
 .drop("CallDate")
 .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
 .drop("WatchDate")
 .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
 "MM/dd/yyyy hh:mm:ss a"))
 .drop("AvailableDtTm")


// COMMAND ----------

// Select the converted columns
fireTsDF
 .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
 .show(5, false)


// COMMAND ----------

//query using functions from spark.sql.functions like year,month,day
fireTsDF
 .select(year($"IncidentDate"))
 .distinct()
 .orderBy(year($"IncidentDate"))
 .show()

// COMMAND ----------

//query using functions from spark.sql.functions like year,month,day
fireTsDF
 .select(year($"IncidentDate"))
 .distinct()
 .orderBy(year($"IncidentDate"))
 .show()

// COMMAND ----------

//what were the most common types of firecalls
fireTsDF
 .select("CallType")
 .where(col("CallType").isNotNull)
 .groupBy("CallType")
 .count()
 .orderBy(desc("count"))
 .show(10, false)


// COMMAND ----------

//uses of statistical methods
import org.apache.spark.sql.{functions => F}
display(fireTsDF
 .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMin"),
 F.min("ResponseDelayedinMin"), F.max("ResponseDelayedinMin")))


// COMMAND ----------

//EJERCICIOS
//What were all the different types of fire calls in 2018?
display(fireTsDF
       .select("CallType")
       .where(year($"IncidentDate") === "2018")
       .distinct())

// COMMAND ----------

//What months within the year 2018 saw the highest number of fire calls?
#what were the most common types of firecalls
display(fire_ts_df
 .select('IncidentDate')
 .where(year('IncidentDate')== "2018")
 .orderBy(month('IncidentDate'), ascending=False)
 .groupBy(month('IncidentDate'))
 .count())

// COMMAND ----------

//creating a row
import org.apache.spark.sql.Row
val row = Row(350, true, "Learning Spark 2E", null)


// COMMAND ----------

//using an index into the row object you can access individual fields with its public getter methods:
row.get(1) 



// COMMAND ----------

//Defining a case class
case class DeviceIoTData (battery_level: Long, c02_level: Long, cca2: String, cca3: String, cn: String, device_id: Long, device_name: String, humidity: Long, ip: String, latitude: Double, lcd: String, longitude: Double, scale:String, temp: Long, timestamp: Long)

// COMMAND ----------

//we can use it to read our file and convert the returned dataset.row into dataset.deviceiotdata
val ds = spark.read
  .json("/databricks-datasets/learning-spark-v2/iot-devices/iot_devices.json")
  .as[DeviceIoTData]

display(ds)


// COMMAND ----------

//Transformations and actions on dataframes
val filterTempDS = ds.filter({d => {d.temp > 30 && d.humidity > 70}})

filterTempDS.show(5, false)

// COMMAND ----------

//Here’s another example that results in another, smaller Dataset:
// In Scala
case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long,
 cca3: String)
val dsTemp = ds
 .filter(d => {d.temp > 25})
 .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
 .toDF("temp", "device_name", "device_id", "cca3")
 .as[DeviceTempByCountry]
  dsTemp.show(5, false)


// COMMAND ----------

val device = dsTemp.first()
println(device)


// COMMAND ----------

//you could express the same query using column names and then cast to dataset
val dsTemp2 = ds
 .select($"temp", $"device_name", $"device_id", $"device_id", $"cca3")
 .where("temp > 25")
 .as[DeviceTempByCountry]


// COMMAND ----------

display(ds)

// COMMAND ----------

//1. Detect failing devices with battery levels below a threshold.
display(ds
        .filter({d => {d.battery_level < 6}})
       .select("device_id")
       .distinct())



// COMMAND ----------

//2. Identify offending countries with high levels of CO2 emissions.
display(ds
        .filter({d => {d.c02_level > 1400}})
       .select("cn","c02_level")
       .orderBy("c02_level")
       .distinct()
      )

// COMMAND ----------

//3. Compute the min and max values for temperature, battery level, CO2, and humidity
import org.apache.spark.sql.{functions => F}

display(ds
       .select(F.max("temp"), F.min("temp"),F.min("battery_level")))


// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

