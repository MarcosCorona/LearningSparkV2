# Databricks notebook source
file = "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
df = spark.read.format("json").load(file)

# COMMAND ----------

#Saving a DataFrame as a JSON file is simple. Specify the appropriate DataFrameWriter methods and arguments, and supply the location to save the JSON files to
(df.write.format("json")
 .mode("overwrite")
 .save("/tmp/data/json/df_json"))

# COMMAND ----------

