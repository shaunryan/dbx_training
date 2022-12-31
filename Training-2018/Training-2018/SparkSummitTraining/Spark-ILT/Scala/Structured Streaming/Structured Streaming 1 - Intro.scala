// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png" style="float: left: margin: 20px"/>
// MAGIC 
// MAGIC # Structured Streaming
// MAGIC 
// MAGIC **This topic is divided into four notebooks**
// MAGIC 0. Introduction to Structured Streaming (this notebook)
// MAGIC 0. Examples using TCP/IP sockets, windows, watermarking & checkpointing
// MAGIC 0. Examples using Kafka
// MAGIC 0. Spark Streaming Lab
// MAGIC 
// MAGIC ## Introduction
// MAGIC ** What you will learn:**
// MAGIC * Micro-Batching
// MAGIC * The Unbounded Input Table
// MAGIC * The Results Table
// MAGIC * Output Modes
// MAGIC * Output Sinks

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## The Problem
// MAGIC 
// MAGIC We have a stream of data coming in from a TCP-IP socket, Kafka, Kinesis or other sources...
// MAGIC 
// MAGIC The data is coming in faster than it can be consumed
// MAGIC 
// MAGIC How do we solve this problem?
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/drinking-from-the-fire-hose.png"/>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## The Micro-Batch Model
// MAGIC 
// MAGIC Many streaming APIs follow the Micro-Batch model.
// MAGIC 
// MAGIC In this model, we take our fire-hose of data and collect data for a set interval of time (the **Trigger Interval**).
// MAGIC 
// MAGIC In our example, the **Trigger Interval** is two seconds.
// MAGIC 
// MAGIC <img style="width:100%" src="https://files.training.databricks.com/images/streaming-timeline.png"/>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Processing the Micro-Batch
// MAGIC 
// MAGIC For each interval, our job is to process the data from the previous [two-second] interval.
// MAGIC 
// MAGIC As we are processing data, the next batch of data is being collected for us.
// MAGIC 
// MAGIC In our example, we are processing two seconds worth of data in about one second.
// MAGIC 
// MAGIC <img style="width:100%" src="https://files.training.databricks.com/images/streaming-timeline-1-sec.png">

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Falling Behind
// MAGIC 
// MAGIC Real quick, what happens if we don't process the data fast enough?
// MAGIC * With a TCP-IP Socket?
// MAGIC * With a Kafka or Kinesis store?
// MAGIC 
// MAGIC <img style="width:100%" src="https://files.training.databricks.com/images/streaming-timeline-2-1-sec.png"/>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## From Micro-Batch to Tables
// MAGIC 
// MAGIC Our goal is simply to process the data for the previous interval before the next batch arrives.
// MAGIC 
// MAGIC The same rules apply, but Structured Streaming asks you to think about the process a little differently.
// MAGIC 
// MAGIC **Treat a live data stream as a table to which data is being continuously updated.**
// MAGIC 
// MAGIC Then express your streaming computation as a standard batch-like query against a static table.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## The Unbounded Input Table
// MAGIC 
// MAGIC Think of the stream as an unbounded **Input Table**. 
// MAGIC 
// MAGIC At each **Trigger Interval**, new rows are being appended to the **Input Table**.
// MAGIC 
// MAGIC Using our previous example, new rows would be appended to the **Input Table** every two seconds.
// MAGIC 
// MAGIC <img  style="width: 600px" src="https://files.training.databricks.com/images/unbounded-input-table.png"/>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Querying the Input Table
// MAGIC 
// MAGIC When you issue a query on the **Input Table**, you generate a **Result Table** which is then written out. 
// MAGIC 
// MAGIC That is to say, at every **Trigger Interval**:
// MAGIC   0. New rows are appended to the **Input Table**.
// MAGIC   0. The query is re-executed.
// MAGIC   0. The **Result Table** table is updated accordingly.
// MAGIC   0. The rows are written to some external output, or **Sink**.
// MAGIC 
// MAGIC <img style="width: 600px" src="https://files.training.databricks.com/images/querying-the-input-table.png"/>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC ## Output Modes
// MAGIC 
// MAGIC Since version 2.1.1, three output modes are supported:
// MAGIC 
// MAGIC | Mode          | Description |
// MAGIC | ------------- | ----------- |
// MAGIC | **Complete**  | The entire updated Result Table is written to the sink. The individual sink implementation decides how to handle writing the entire table. |
// MAGIC | **Append**    | Only the new rows appended to the Result Table since the last trigger are written to the sink. This is only useful if the existing rows in the Result Table are not changed. |
// MAGIC | **Update**    | Only the rows in the Result Table that were updated since the last trigger will be outputted to the sink. (Available since Spark 2.1.1) |

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Output Sinks
// MAGIC 
// MAGIC The output, or **Sink**, is the external storage. 
// MAGIC 
// MAGIC As of version 2.3, the following implementations are supported:
// MAGIC 
// MAGIC | Sink Type   | Supported Output Modes          | Description |
// MAGIC | ----------- | ------------------------------- | ----------- |
// MAGIC | **File**    | Append                          | Dumps the Result Table to a file. In Spark 2.0 and 2.1, only supports Parquet, but now also supports JSON, CSV, etc. Supports writing to partitioned tables. |
// MAGIC | **Kafka**   | Append, Complete, Update        | Stores the output to one or more topics in Kafka. (Supported via the `spark-sql-kafka-0-10` add-in module.) |
// MAGIC | **Console** | Append, Complete, Update        | Writes the data to the console. Only useful for debugging. |
// MAGIC | **Memory**  | Append, Complete                | Writes the data to an in-memory table, which can be queried through Spark SQL or the DataFrame API. |
// MAGIC | **Foreach** | Append, Complete, Update        | This is your "escape hatch", allowing you to write your own type of sink. |
// MAGIC 
// MAGIC 
// MAGIC **NOTE:** Within Databricks, the **Console** sink doesn't work very well, however, we _can_ pass the streaming DataFrame to the `display()` function which makes use of a **Memory** sink. We will see this in action a bit later.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Next Steps
// MAGIC 
// MAGIC * Structured Streaming 1 - Intro
// MAGIC * [Structured Streaming 2 - TCP/IP]($./Structured Streaming 2 - TCPIP)
// MAGIC * [Structured Streaming 3 - Kafka]($./Structured Streaming 3 - Kafka)
// MAGIC * [Structured Streaming 4 - Lab]($./Structured Streaming 4 - Lab)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>