# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Databricks Delta Streaming
# MAGIC Databricks&reg; Delta allows you to work with streaming data.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Read and write streaming data into a data lake
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers 
# MAGIC * Secondary Audience: Data Analyst sand Data Scientists
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
# MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
# MAGIC * Databricks Runtime 4.2 or greater
# MAGIC * Completed courses Spark-SQL, DataFrames or ETL-Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>, or have similar knowledge
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC Data from 
# MAGIC `/mnt/training/definitive-guide/data/activity-data`
# MAGIC contains smartphone accelerometer samples from all devices and users. 
# MAGIC 
# MAGIC The file consists of the following columns:
# MAGIC 
# MAGIC | Field           | Description                  |
# MAGIC |-----------------|------------------------------|
# MAGIC | `Arrival_Time`  | when record came in          |
# MAGIC | `Creation_Time` | when record was created      |
# MAGIC | `Index`         | unique identifier of event   |
# MAGIC | `x`             | acceleration in x-dir        |
# MAGIC | `y`             | acceleration in y-dir        |
# MAGIC | `z`             | acceleration in z-dir        |
# MAGIC | `User`          | unique user identifier       |
# MAGIC | `Model`         | i.e Nexus4                   |
# MAGIC | `Device`        | type of Model                |
# MAGIC | `gt`            | ground truth                 |
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The last column, `gt`, "ground truth" means
# MAGIC * What the person was ACTUALLY doing when the measurement was taken, in this case, walking or going up stairs, etc..
# MAGIC * Wikipedia: <a href="https://en.wikipedia.org/wiki/Ground_truth" target="_blank">Ground Truth</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup-05"

# COMMAND ----------

# MAGIC %md
# MAGIC Set up relevant paths.

# COMMAND ----------

dataPath = "dbfs:/mnt/training/definitive-guide/data/activity-data"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Streaming Concepts
# MAGIC 
# MAGIC <b>Stream processing</b> is where you continuously incorporate new data into a data lake and compute results.
# MAGIC 
# MAGIC The data is coming in faster than it can be consumed.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/firehose.jpeg" style="height: 200px"/></div><br/>
# MAGIC 
# MAGIC Treat a <b>stream</b> of data as a table to which data is continously appended. 
# MAGIC 
# MAGIC In this course we are assuming Databricks Structured Streaming, which uses the DataFrame API. 
# MAGIC 
# MAGIC There are other kinds of streaming systems.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/stream2rows.png" style="height: 300px"/></div><br/>
# MAGIC 
# MAGIC Examples are bank card transactions, Internet of Things (IoT) device data, and video game play events. 
# MAGIC 
# MAGIC Data coming from a stream is typically not ordered in any way.
# MAGIC 
# MAGIC A streaming system consists of 
# MAGIC * <b>Input source</b> such as Kafka, Azure Event Hub, files on a distributed system or TCP-IP sockets
# MAGIC * <b>Sinks</b> such as Kafka, Azure Event Hub, various file formats, `foreach` sinks, console sinks or memory sinks
# MAGIC 
# MAGIC ### Streaming and Databricks Delta
# MAGIC 
# MAGIC In streaming, the problems of traditional data pipelines are exacerbated. 
# MAGIC 
# MAGIC Specifically, with frequent meta data refreshes, table repairs and accumulation of small files on a secondly- or minutely-basis!
# MAGIC 
# MAGIC Many small files result because data (may be) streamed in at low volumes with short triggers.
# MAGIC 
# MAGIC Databricks Delta is uniquely designed to address these needs.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### READ Stream using Databricks Delta
# MAGIC 
# MAGIC The `readStream` method is similar to a transformation that outputs a DataFrame with specific schema specified by `.schema()`. 
# MAGIC 
# MAGIC Each line of the streaming data becomes a row in the DataFrame once `writeStream` is invoked.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> In this lesson, we limit flow of stream to one file per trigger with `option("maxFilesPerTrigger", 1)` so that you do not exceed file quotas you may have on your end. The default value is 1000.
# MAGIC 
# MAGIC Notice that nothing happens until you engage an action, i.e. a `writeStream` operation a few cells down.
# MAGIC 
# MAGIC Do some data normalization as well:
# MAGIC * Convert `Arrival_Time` to `timestamp` format.
# MAGIC * Rename `Index` to `User_ID`.

# COMMAND ----------

static = spark.read.json(dataPath)
dataSchema = static.schema

deltaStreamWithTimestampDF = (spark
  .readStream
  .option("maxFilesPerTrigger", 1)
  .schema(dataSchema)
  .json(dataPath)
  .withColumnRenamed('Index', 'User_ID')
  .selectExpr("*","cast(cast(Arrival_Time as double)/1000 as timestamp) as event_time")
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### WRITE Stream using Databricks Delta
# MAGIC 
# MAGIC #### General Notation
# MAGIC Use this format to write a streaming job to a Databricks Delta table.
# MAGIC 
# MAGIC > `(myDF` <br>
# MAGIC   `.writeStream` <br>
# MAGIC   `.format("delta")` <br>
# MAGIC   `.option("checkpointLocation", somePath)` <br>
# MAGIC   `.outputMode("append")` <br>
# MAGIC   `.table("my_table")` or `.start(path)` <br>
# MAGIC `)`
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> If you use the `.table()` notation, it will write output to a default location. 
# MAGIC * This would be in parquet files under `/user/hive/warehouse/default.db/my_table`
# MAGIC 
# MAGIC In this course, we want everyone to write data to their own directory; so, instead, we use the `.start()` notation.
# MAGIC 
# MAGIC #### Output Modes
# MAGIC Notice, besides the "obvious" parameters, specify `outputMode`, which can take on these values
# MAGIC * `append`: add only new records to output sink
# MAGIC * `complete`: rewrite full output - applicable to aggregations operations
# MAGIC * `update`: update changed records in place
# MAGIC 
# MAGIC #### Checkpointing
# MAGIC 
# MAGIC When defining a Delta streaming query, one of the options that you need to specify is the location of a checkpoint directory.
# MAGIC 
# MAGIC `.writeStream.format("delta").option("checkpointLocation", <path-to-checkpoint-directory>) ...`
# MAGIC 
# MAGIC This is actually a structured streaming feature. It stores the current state of your streaming job.
# MAGIC 
# MAGIC Should your streaming job stop for some reason and you restart it, it will continue from where it left off.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> If you do not have a checkpoint directory, when the streaming job stops, you lose all state around your streaming job and upon restart, you start from scratch.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Also note that every streaming job should have its own checkpoint directory: no sharing.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Let's Do Some Streaming
# MAGIC 
# MAGIC In the cell below, we write streaming query to a Databricks Delta table. 
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Notice how we do not need to specify a schema: it is inferred from the data! 

# COMMAND ----------

writePath = basePath + "/output"
checkpointPath = writePath + "/_checkpoint"

deltaStreamingQuery = (deltaStreamWithTimestampDF
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpointPath)
  .outputMode("append")
  .queryName("stream_1p")
  .start(writePath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC See list of active streams.

# COMMAND ----------

for s in spark.streams.active:
  print("{}: {}".format(s.name, s.id))

# COMMAND ----------

# MAGIC %md
# MAGIC Wait until stream is done initializing...

# COMMAND ----------

untilStreamIsReady("stream_1p")

# COMMAND ----------

for s in spark.streams.active:
  print("Stopping stream: {}".format(s.name))
  s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC # LAB

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 1: Table-to-Table Stream
# MAGIC 
# MAGIC Here we read a stream of data from from `writePath` and write another stream to `activityPath`.
# MAGIC 
# MAGIC The data consists of a grouped count of `gt` events.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Make sure the stream using `deltaStreamingQuery` is still running!
# MAGIC 
# MAGIC To perform an aggregate operation, what kind of `outputMode` should you use?

# COMMAND ----------

# ANSWER
activityPath   = basePath + "/activityCount"
checkpointPath = activityPath + "/_checkpoint"

activityCountsQuery = (spark.readStream
  .format("delta")
  .load(str(writePath))   
  .groupBy("gt")
  .count()
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpointPath)
  .outputMode("complete")
  .queryName("stream_2p")
  .start(activityPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Wait until stream is done initializing...

# COMMAND ----------

untilStreamIsReady("stream_2p")

# COMMAND ----------

# TEST - Run this cell to test your solution.
activityQueryTruth = spark.streams.get(activityCountsQuery.id).isActive

dbTest("Delta-05-activityCountsQuery", True, activityQueryTruth)

print("Tests passed!")

# COMMAND ----------

for s in spark.streams.active:
  print("Stopping stream: {}".format(s.name))
  s.stop()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 2
# MAGIC 
# MAGIC Plot the occurrence of all events grouped by `gt`.
# MAGIC 
# MAGIC Under <b>Plot Options</b>, use the following:
# MAGIC * <b>Series Groupings:</b> `gt`
# MAGIC * <b>Values:</b> `count`
# MAGIC 
# MAGIC In <b>Display type</b>, use <b>Bar Chart</b> and click <b>Apply</b>.
# MAGIC 
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/ch5-plot-options.png"/></div><br/>
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> In the cell below, we use the `withWatermark` and `window` methods, which aren't covered in this course. 
# MAGIC 
# MAGIC To learn more about watermarking, please see <a href="https://databricks.com/blog/2017/05/08/event-time-aggregation-watermarking-apache-sparks-structured-streaming.html" target="_blank">Event-time Aggregation and Watermarking in Apache Spark’s Structured Streaming</a>.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import hour, window, col

countsDF = (deltaStreamWithTimestampDF      
  .withWatermark("event_time", "180 minutes")
  .groupBy(window("event_time", "60 minute"), "gt")
  .count()
)

display(countsDF.withColumn('hour',hour(col('window.start'))), streamName = "stream_3p")

# COMMAND ----------

untilStreamIsReady("stream_3p")

# COMMAND ----------

# TEST - Run this cell to test your solution.
schemaStr = str(countsDF.schema)

dbTest("Assertion #1", 3, len(countsDF.columns))
dbTest("Assertion #2", True, "(gt,StringType,true)" in schemaStr) 
dbTest("Assertion #3", True, "(count,LongType,false)" in schemaStr) 

dbTest("Assertion #5", True, "window,StructType" in schemaStr)
dbTest("Assertion #6", True, "(start,TimestampType,true)" in schemaStr) 
dbTest("Assertion #7", True, "(end,TimestampType,true)" in schemaStr) 

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Shut Down
# MAGIC Stop streams.

# COMMAND ----------

# ANSWER

for s in spark.streams.active:
  print("Stopping stream: {}".format(s.name))
  s.stop()

# COMMAND ----------

# TEST - Run this cell to test your solution.
numActiveStreams = len(spark.streams.active)
dbTest("Delta-05-numActiveStreams", 0, numActiveStreams)

print("Tests passed!")

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Cleanup-05"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC In this lesson, we:
# MAGIC * Treated a stream of data as a table to which data is continously appended. 
# MAGIC * Learned we could write Databricks Delta output to a directory or directly to a table.
# MAGIC * Used Databricks Delta's `complete` mode with aggregation queries.
# MAGIC * Saw that `display` will produce LIVE graphs if we feed it a streaming DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="https://docs.databricks.com/delta/delta-streaming.html#as-a-sink" target="_blank">Delta Streaming Write Notation</a>
# MAGIC * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#" target="_blank">Structured Streaming Programming Guide</a>
# MAGIC * <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">A Deep Dive into Structured Streaming</a> by Tagatha Das. This is an excellent video describing how Structured Streaming works.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>