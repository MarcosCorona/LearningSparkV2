// Databricks notebook source
/*Descargar el Quijote 
https://gist.github.com/jsdario/6d6c69398cb0c73111e49f1218960f79
Aplicar no solo count (para obtener el número de líneas) y show sino probar distintas sobrecargas del método show (con/sin truncate, indicando/sin indicar num de filas, etc) así como también los métodos, head, take, first (diferencias entre estos 3?*/

//directorio para usar si queremos leer:/FileStore/shared_uploads/marcos.corona@bosonit.com/el_quijote.txt
val quijote = spark.read.text("/FileStore/shared_uploads/marcos.corona@bosonit.com/el_quijote.txt")

// COMMAND ----------

quijote.count()

// COMMAND ----------

quijote.show()

// COMMAND ----------

//muestra filas y columnas verticalmente
quijote.show(true)

// COMMAND ----------

//muestra todos los datos de cada columna
//si no mostramos un nº de filas colapsa
quijote.show(2,truncate=false)

// COMMAND ----------

//si el truncar es verdadero, no los muestra.
quijote.show(truncate=true)

// COMMAND ----------

//coge la primera fila o las primeras en cuestion del nº especificado dentro de head
quijote.head()

// COMMAND ----------

//selecciona la fila que especifiques o todo el texto en este caso
quijote.take(1)

// COMMAND ----------

//lo mismo que head pero solo la primera fila. si intentas coger 2 da error.
quijote.first()

// COMMAND ----------

//PASAMOS AL EJERCICIO DE MNM
//PRIMERO VOY A TRAER AQUÍ EL EJEMPLO DESARROLLADO.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder.appName("MnMCount").getOrCreate()

val mnmDF = spark.read
.option("header","true")
.option("inferSchema","true")
.csv("/FileStore/shared_uploads/marcos.corona@bosonit.com/mnm_dataset.csv")
/*Otras operaciones de agregación como el Max con otro tipo de 
ordenamiento (descendiente).*/

val countMnMDF=mnmDF
  .select("State","Color","Count")
  .groupBy("State","Color")
  .agg(count("Count").alias("Total"))
  .orderBy(asc("Total"))


countMnMDF.show(60)
println(s"Total Rows = ${countMnMDF.count()}")
println()


// COMMAND ----------

/*hacer un ejercicio como el “where” de CA que aparece en el libro pero indicando más opciones de estados (p.e. NV, TX, CA, CO).*/
val whereDF =mnmDF
  .select("State","Color","Count")
  .where($"State" === "NV" || $"State" === "TX")
  .groupBy("State","Color")
  .agg(count("Count").alias("Total"))
  .orderBy(asc("Total"))
whereDF.show()

// COMMAND ----------

/*Hacer un ejercicio donde se calculen en una misma operación el Max, Min, Avg, Count. Revisar el API (documentación) donde encontrarán este ejemplo:ds.agg(max($"age"), avg($"salary"))
ds.groupBy().agg(max($"age"), avg($"salary") NOTA: $ es un alias de col()*/
import pyspark.sql.functions.F
val calculos = mnmDF
.select(F.sum("Count"), F.avg("Count"),
F.min("Count"), F.max("Count"))
.show()





// COMMAND ----------

l("SELECT State, Color, Count FROM Table GROUPBY State, Color having to count(Count) as Total ORDER BY Total;")