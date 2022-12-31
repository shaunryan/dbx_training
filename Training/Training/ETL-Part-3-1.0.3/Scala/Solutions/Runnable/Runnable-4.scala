// Databricks notebook source
// MAGIC 
// MAGIC %scala
// MAGIC dbutils.widgets.text("username", "")
// MAGIC dbutils.widgets.text("ranBy", "")

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions.{lit, unix_timestamp}
// MAGIC import org.apache.spark.sql.types.TimestampType
// MAGIC 
// MAGIC val username = dbutils.widgets.get("username")
// MAGIC val ranBy = dbutils.widgets.get("ranBy")
// MAGIC val path = username+"/academy/runLog.parquet"
// MAGIC 
// MAGIC spark
// MAGIC   .range(1)
// MAGIC   .select(unix_timestamp.alias("runtime").cast(TimestampType), lit(ranBy).alias("ranBy"))
// MAGIC   .write
// MAGIC   .mode("APPEND")
// MAGIC   .parquet(path)
// MAGIC 
// MAGIC display(spark.read.parquet(path))

// COMMAND ----------

// MAGIC %scala
// MAGIC dbutils.notebook.exit(path)