// Databricks notebook source
// DBTITLE 1,Reading a binary file into a dataframe
/*To read binary files, specify the data source format as a binaryFile. You can load files with paths matching a given global pattern while preserving the behavior of partition discovery with the data source option pathGlobFilter*/
val path = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
val binaryFilesDF = spark.read.format("binaryFile")
 .option("pathGlobFilter", "*.jpg")
 .load(path)
binaryFilesDF


// COMMAND ----------

display(binaryFilesDF)

// COMMAND ----------

