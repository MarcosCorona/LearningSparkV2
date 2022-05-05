# Databricks notebook source
#Carácterísticas asociadas a un RDD:
#Dependencias.
#Particiones.
#Compute Fuction: partition => Iterator[T]

#Primero, se requiere una lista de dependencias que indique spark, cuando sea necesario reproducir los resultados Spark puede recrear un RDD a partir de estas dependencias  y replicar operaciones.

#Segundo, las particiones proporcionan la capacidad de dividir el trabajo para paralelizar el cálculo en particiones entre ejecutors.

#Tercero, un rdd tiene una función de cóputo que produce un Iterator para los datos que se almacenan en el RDD.

#Inconvenientes: 

#La función función de calculo es opaca a Spark, es decir, Spark no sabe lo que estamos haciendo con dicha funcion.

#El tipo de datos Iterator también es opaco para los RDD de python, Spark solo sabe que es un objeto genérico de pyhton.

#Debido a que no se puede inspeccionar el cálculo o la expresión en la funcion, Spark no tiene forma de optimizar dicho cálculo, ya que no tiene comprensión de su intención.

#Finalmente Spark no tiene conocimiento del tipo de datos específico en T, el iterador. Para Spark es un objeto opaco.

#Por lo tanto todo lo que spark puede hacer es serializar un objeto opaco como una serie de byts.

#Key schemes para estructurar Spark:

#Expresar cálculos mediante el uso de patrones comunes que se encuentran en el análisis de datos. Estos patrones se expresan como operaciones, como filtrar, seleccionar, contar, agregar,promediar y agrupar. Esto proporciona mayor claridad y simplicidad.





    

# COMMAND ----------

#HIGH LEVEL
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
# Create a DataFrame using SparkSession
spark = (SparkSession
 .builder
 .appName("AuthorsAges")
 .getOrCreate())
# Create a DataFrame 
data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)], ["name", "age"])
# Group the same names together, aggregate their ages, and compute an average
avg_df = data_df.groupBy("name").agg(avg("age"))
# Show the results of the final execution
avg_df.show()

# COMMAND ----------

#key merits and benefits
#Aggregating all the ages foreach name, group by name,
#and then average the ages.

#LOW LEVEL:

#create an rdd of tuples(name,age)

dataRDD = sc.parallelize([("Brooke",20),("Denny",31),("Jules",30),("TD",35),("Brooke",25)])

#Use map and reduceByKey transformations with their lambda
#expressions to aggregate and then compute average.

agesRDD = (dataRDD
    .map(lambda x: (x[0],(x[1],1)))
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    .map(lambda x: (x[0],x[1][0]/x[1][1])))

# COMMAND ----------

#WAYS TO DEFINE AN SCHEMA
#1 way is define it programmatically, the example will have 3 columns,author,title and pages.
from pyspark.sql.types import *
schema = StructType([StructField("author", StringType(), False),
 StructField("title", StringType(), False),
 StructField("pages", IntegerType(), False)])


# COMMAND ----------

#Define the sime schema using DDL is much simpler:
from pyspark.sql import SparkSession

# Define schema for our data using DDL 
schema = "`Id` INT, `First` STRING, `Last` STRING, `Url` STRING, `Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"
# Create our static data
data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter","LinkedIn"]],
 [2, "Brooke","Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter",
"LinkedIn"]],
 [3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web",
"twitter", "FB", "LinkedIn"]],
 [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568,
["twitter", "FB"]],
 [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web",
"twitter", "FB", "LinkedIn"]],
 [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568,
["twitter", "LinkedIn"]]
 ]
# Main program
if __name__ == "__main__":
 # Create a SparkSession
 spark = (SparkSession
 .builder
 .appName("Example-3_6")
 .getOrCreate())
 # Create a DataFrame using the schema defined above
 blogs_df = spark.createDataFrame(data, schema)
 # Show the DataFrame; it should reflect our table above
 blogs_df.show()
 # Print the schema used by Spark to process the DataFrame
 print(blogs_df.printSchema())

    


# COMMAND ----------

from pyspark.sql import Row
blog_row = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
 ["twitter", "LinkedIn"])
#access using index for individual items
blog_row[1]

# COMMAND ----------

#row objects can be used to create DataFrames:
rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
authors_df = spark.createDataFrame(rows, ["Authors", "State"])
authors_df.show()

# COMMAND ----------

#setting a new schema 
from pyspark.sql.types import *
# Programmatic way to define a schema 
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
 StructField('UnitID', StringType(), True),
 StructField('IncidentNumber', IntegerType(), True),
 StructField('CallType', StringType(), True), 
 StructField('CallDate', StringType(), True), 
 StructField('WatchDate', StringType(), True),
 StructField('CallFinalDisposition', StringType(), True),
 StructField('AvailableDtTm', StringType(), True),
 StructField('Address', StringType(), True), 
 StructField('City', StringType(), True), 
 StructField('Zipcode', IntegerType(), True), 
 StructField('Battalion', StringType(), True), 
 StructField('StationArea', StringType(), True), 
 StructField('Box', StringType(), True), 
 StructField('OriginalPriority', StringType(), True), 
 StructField('Priority', StringType(), True), 
 StructField('FinalPriority', IntegerType(), True), 
 StructField('ALSUnit', BooleanType(), True), 
 StructField('CallTypeGroup', StringType(), True),
 StructField('NumAlarms', IntegerType(), True),
 StructField('UnitType', StringType(), True),
 StructField('UnitSequenceInCallDispatch', IntegerType(), True),
 StructField('FirePreventionDistrict', StringType(), True),
 StructField('SupervisorDistrict', StringType(), True),
 StructField('Neighborhood', StringType(), True),
 StructField('Location', StringType(), True),
 StructField('RowID', StringType(), True),
 StructField('Delay', FloatType(), True)])
# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "/FileStore/shared_uploads/marcos.corona@bosonit.com/sf_fire_calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)




# COMMAND ----------

display(fire_df)

# COMMAND ----------

#saving a dataframe as a parquet file or sql table
#parquet_path = "firedb2"
#fire_df.write.format("parquet").save(parquet_path)

# COMMAND ----------

#In Python to save as a table 
#parquet_table = "Fire" # name of the table
#fire_df.write.format("parquet").saveAsTable(parquet_table)

# COMMAND ----------

few_fire_df = (fire_df
 .select("IncidentNumber", "AvailableDtTm", "CallType")
 .where(fire_df["CallType"]!= "Medical Incident"))
few_fire_df.show(5, truncate=False)


# COMMAND ----------

#How many distinct calltypes were recorded as the causes of the fire calls 
#return number of distinct types of calls using countDistinct()
from pyspark.sql.functions import *
(fire_df.select("CallType")
 .where(col("CallType").isNotNull())
 .agg(countDistinct("CallType").alias("DistinctCallTypes"))
 .show())

# COMMAND ----------

#diferents uses of distinct call
(fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .distinct()
 .show(10, False))

# COMMAND ----------

#Renaming columns:
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
(new_fire_df
 .select("ResponseDelayedinMins")
 .where(col("ResponseDelayedinMins") > 5)
 .show(5, False))



# COMMAND ----------

#date conversion
fire_ts_df = (new_fire_df
 .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
 .drop("CallDate")
 .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
 .drop("WatchDate")
 .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
 "MM/dd/yyyy hh:mm:ss a"))
 .drop("AvailableDtTm"))
# Select the converted columns
(fire_ts_df
 .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
 .show(5, False))

# COMMAND ----------

#query using functions from spark.sql.functions like year,month,day
(fire_ts_df
 .select(year('IncidentDate'))
 .distinct()
 .orderBy(year('IncidentDate'))
 .show())



# COMMAND ----------

#what were the most common types of firecalls
(fire_ts_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending=False)
 .show(n=10, truncate=False))


# COMMAND ----------

#Uses of statistical methods examples:
import pyspark.sql.functions as F
display((fire_ts_df
 .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),F.min("ResponseDelayedinMins"),F.max("ResponseDelayedinMins"))))

# COMMAND ----------

#EJERCICIOS
#What were all the different types of fire calls in 2018?
display(fire_ts_df
       .select("CallType")
       .where(year('IncidentDate') == "2018")
       .distinct())

# COMMAND ----------

#What months within the year 2018 saw the highest number of fire calls?
#what were the most common types of firecalls
display(fire_ts_df
 .select('IncidentDate')
 .where(year('IncidentDate')== "2018")
 .orderBy(month('IncidentDate'), ascending=False)
 .groupBy(month('IncidentDate'))
 .count())
 


# COMMAND ----------

#Which neighborhood in San Francisco generated the most fire calls in 2018?
display(fire_ts_df
 .select("Neighborhood","CallNumber")
 .where(fire_df["City"] == "SF")
 .groupBy("Neighborhood")
 .count()
 .orderBy("count", ascending=False)
)

# COMMAND ----------

#Which neighborhoods had the worst response times to fire calls in 2018?
import pyspark.sql.functions as F
display(fire_ts_df
 .select("Neighborhood","ResponseDelayedinMins")
 .where(fire_df["City"] == "SF")
 .groupBy("Neighborhood")
 .agg(F.sum("ResponseDelayedinMins").alias("suma"))
 .orderBy("suma",ascending=False)
)


# COMMAND ----------

#Which week in the year in 2018 had the most fire calls
display(fire_ts_df
        .select('IncidentDate')
        .where(year('IncidentDate') == "2018")
        .groupBy(weekofyear('IncidentDate'))
        .count())

# COMMAND ----------

display(fire_ts_df
        .select('Neighborhood','ZipCode','CallNumber')
       )
#its a primarykey



# COMMAND ----------

#parquet_tableEx = "ejercicio" 
#fire_ts_df.write.format("parquet").saveAsTable(parquet_table#Ex)

# COMMAND ----------

#Creating a row
from pyspark.sql import Row
row = Row(350, True, "Learning Spark 2E", None)

# COMMAND ----------

#using an index into the row object you can access individual fields with its public getter methods:
row[0], row[1]

# COMMAND ----------

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
if __name__ == "__main__":

    spark = (SparkSession
        .builder
        .appName("PythonMnMCount")
        .getOrCreate())
    
    mnm_df=(spark.read
        .option("header","true")
        .option("inferSchema","true")
        .csv("/FileStore/shared_uploads/marcos.corona@bosonit.com/mnm_dataset.csv"))
    
    count_mnm_df = (mnm_df
         .select("State","Color","Count")
         .groupBy("State","Color")
         .agg(count("Count").alias("Total"))
         .orderBy("Total", ascending=False))
    
    count_mnm_df.show(n=60,truncate=False)
    
    print("Total Rows = %d" % (count_mnm_df.count()))
    
    ca_count_mnm_df = (mnm_df
          .select("State", "Color", "Count")
          .where(mnm_df.State == "CA")
          .groupBy("State", "Color")
          .agg(count("Count").alias("Total"))
          .orderBy("Total", ascending=False))
    
    ca_count_mnm_df.show(n=10, truncate=False)


# COMMAND ----------

# Similarities with sql:
count_mnm_df = (mnm_df
 .select("State", "Color", "Count")
 .groupBy("State", "Color")
 .agg(count("Count")
 .alias("Total"))
 .orderBy("Total", ascending=False))

# COMMAND ----------

display(count_mnm_df)

# COMMAND ----------

#SAVING MNM TABLE
parquet_table = "MNM_TABLE" # name of the table
count_mnm_df.write.format("parquet").saveAsTable(parquet_table)

# COMMAND ----------

#Using the method explain(true) to see the different stages of python code goes through, or to get a look of different logical and physicañl plans.
count_mnm_df.explain(True)

# COMMAND ----------

