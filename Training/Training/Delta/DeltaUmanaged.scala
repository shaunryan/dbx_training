// Databricks notebook source
// MAGIC %python
// MAGIC 
// MAGIC events = spark.read.json("/databricks-datasets/structured-streaming/events/")
// MAGIC events.write.format("delta").save("/delta/events")
// MAGIC spark.sql("CREATE TABLE events USING DELTA LOCATION '/delta/events/'")

// COMMAND ----------

// MAGIC %sql
// MAGIC drop table events

// COMMAND ----------

dbutils.fs.unmount("/mnt/ex_data")

// COMMAND ----------

display(dbutils.fs.mounts)

// COMMAND ----------

dbutils.fs.rm("/output.csv", true)

// COMMAND ----------

display(dbutils.fs.ls("/mnt"))
//dbutils.fs.rm("/data", true)
