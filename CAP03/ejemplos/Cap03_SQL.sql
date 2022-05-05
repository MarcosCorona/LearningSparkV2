-- Databricks notebook source
/*Similarities with pyspark*/
SELECT State, Color, Total, sum(Total) AS Total
FROM mnm_table
GROUP BY State, Color, Total
ORDER BY Total DESC


-- COMMAND ----------

