// Databricks notebook source
// DBTITLE 1,Reading an image file into a dataframe
import org.apache.spark.ml.source.image

val imageDir = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"

val imagesDF = spark.read.format("image").load(imageDir)

imagesDF.printSchema

display(imagesDF.select("image.height", "image.width", "image.nChannels", "image.mode",
 "label").take(5))




// COMMAND ----------

//display the photos:
display(imagesDF.take(5))

// COMMAND ----------

