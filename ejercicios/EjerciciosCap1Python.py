# Databricks notebook source
quijote = spark.read.text("/FileStore/shared_uploads/marcos.corona@bosonit.com/el_quijote.txt")

# COMMAND ----------

quijote.show()

# COMMAND ----------

