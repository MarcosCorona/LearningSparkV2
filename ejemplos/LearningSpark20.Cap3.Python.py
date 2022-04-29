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

