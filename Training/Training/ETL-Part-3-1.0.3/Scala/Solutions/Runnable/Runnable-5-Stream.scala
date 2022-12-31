// Databricks notebook source

spark.sql("set spark.databricks.delta.preview.enabled=true") // Confirm Delta is enabled

// COMMAND ----------

// ANSWER

dbutils.widgets.text("readPath", "")
dbutils.widgets.text("writePath", "")
dbutils.widgets.text("checkpointLocation", "")

// COMMAND ----------

// ANSWER

val readPath = dbutils.widgets.get("readPath")
val writePath = dbutils.widgets.get("writePath")
val checkpointLocation = dbutils.widgets.get("checkpointLocation")

// COMMAND ----------

try {
  // Make sure the readPath actually exists
  dbutils.fs.ls(readPath)
} catch {
  case e:Exception => dbutils.notebook.exit("Failed to find readPath")
}

// COMMAND ----------

// ANSWER

val ddlSchema = "`tweet_id` BIGINT,`text` STRING,`screen_name` STRING,`user_id` BIGINT,`lang` STRING"

val tweetsDF = spark
  .readStream
  .option("path", readPath)
  .format("json")
  .schema(ddlSchema)
  .load()

// COMMAND ----------

// ANSWER

import org.apache.spark.sql.streaming.Trigger

val sq = (tweetsDF
  .writeStream
  .trigger(Trigger.Once())
  .format("delta") // This can be parquet if Delta is not enabled
  .option("checkpointLocation", checkpointLocation)
  .option("path", writePath)
  .outputMode("append")
  .start()
)
sq.awaitTermination()

// COMMAND ----------

val count = spark.read.format("delta").load(writePath).count()

// COMMAND ----------

// ANSWER
dbutils.notebook.exit(count.toString)
