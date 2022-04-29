# Databricks notebook source
x = 3
y = 2
x*y

# COMMAND ----------

print(len(sys.argv),sys.argv[3])

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

    spark.stop()
    

# COMMAND ----------



# COMMAND ----------

