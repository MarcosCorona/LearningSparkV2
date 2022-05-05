// Databricks notebook source
//You can read a JSON file into a DataFrame the same way you did with Parquet—just specify "json" in the format() method
val file = "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
val df = spark.read.format("json").load(file)
