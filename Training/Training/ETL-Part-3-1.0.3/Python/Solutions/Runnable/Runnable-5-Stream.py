# Databricks notebook source

spark.sql("set spark.databricks.delta.preview.enabled=true") # Confirm Delta is enabled

# COMMAND ----------

# ANSWER

dbutils.widgets.text("readPath", "")
dbutils.widgets.text("writePath", "")
dbutils.widgets.text("checkpointLocation", "")

# COMMAND ----------

# ANSWER

readPath = dbutils.widgets.get("readPath")
writePath = dbutils.widgets.get("writePath")
checkpointLocation = dbutils.widgets.get("checkpointLocation")

# COMMAND ----------

try:
  # Make sure the readPath actually exists
  dbutils.fs.ls(readPath)
except:
  dbutils.notebook.exit("Failed to find readPath")

# COMMAND ----------

# ANSWER

ddlSchema = "`tweet_id` BIGINT,`text` STRING,`screen_name` STRING,`user_id` BIGINT,`lang` STRING"

tweetsDF = (spark
  .readStream
  .option("path", readPath)
  .format("json")
  .schema(ddlSchema)
  .load()
)

# COMMAND ----------

# ANSWER

sq = (tweetsDF
  .writeStream
  .trigger(once=True)
  .format("delta") # This can be parquet if Delta is not enabled
  .option("checkpointLocation", checkpointLocation)
  .option("path", writePath)
  .outputMode("append")
  .start()
)
sq.awaitTermination()

# COMMAND ----------

try:
  dbutils.fs.ls(writePath)
except:
  dbutils.notebook.exit("Failed to find writePath")

# COMMAND ----------

count = spark.read.format("delta").load(writePath).count()

# COMMAND ----------

# ANSWER
dbutils.notebook.exit(str(count))
