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
blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()





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
blogsDF.sort(col("Id").desc).show()

// COMMAND ----------

//Rows are objects in spark and an orderer collection of fields, you can instantiate a row in each of Spark's supported languages and access its fields by an index starting at 0:
import org.apache.spark.sql.Row

//create a row
val blogRow = (6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",Array("twitter", "LinkedIn"))

//access using index for individual items
println(blogRow)



// COMMAND ----------

