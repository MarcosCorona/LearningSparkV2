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

//AN easy way to define the schema:
// In Scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.rdd.RDD

 val spark = SparkSession
 .builder
 .appName("Example-3_7")
 .getOrCreate()

 // Get the path to the JSON file
 val jsonFile = spark.read
  .option("header","true")
  .option("inferSchema","true")
.json("/FileStore/shared_uploads/marcos.corona@bosonit.com/blogs.json")

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


