// Databricks notebook source

// ANSWER
dbutils.widgets.dropdown("hour", "00", (0 to 23).map(x => f"${x}%02d"))
dbutils.widgets.text("output_path", "/tmp/logByHour.parquet")

// COMMAND ----------

// ANSWER
val df = spark.read.parquet("/mnt/training/EDGAR-Log-20170329/enhanced/EDGAR-Log-20170329-sample.parquet")

// COMMAND ----------

// ANSWER
val hour = dbutils.widgets.get("hour")
val filteredDF = df.filter($"time".like(s"$hour%"))

// COMMAND ----------

// ANSWER
val output_path = dbutils.widgets.get("output_path")
filteredDF.write.mode("OVERWRITE").parquet(output_path)

// COMMAND ----------

// ANSWER
dbutils.notebook.exit(output_path)
