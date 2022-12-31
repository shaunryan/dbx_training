# Databricks notebook source

# ANSWER
dbutils.widgets.dropdown("hour", "00", [str(i).zfill(2) for i in range(24)])
dbutils.widgets.text("output_path", "/tmp/logByHour.parquet")

# COMMAND ----------

# ANSWER
df = spark.read.parquet("/mnt/training/EDGAR-Log-20170329/enhanced/EDGAR-Log-20170329-sample.parquet")

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col

hour = dbutils.widgets.get("hour")
filteredDF = df.filter(col("time").like("{}%".format(hour)))

# COMMAND ----------

# ANSWER
output_path = dbutils.widgets.get("output_path")
filteredDF.write.mode("OVERWRITE").parquet(output_path)

# COMMAND ----------

# ANSWER
dbutils.notebook.exit(output_path)
