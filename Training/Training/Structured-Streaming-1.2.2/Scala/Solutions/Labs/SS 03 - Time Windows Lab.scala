// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img src="https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png" style="float: left: margin: 20px"/>
// MAGIC 
// MAGIC # Working with Time Windows Lab
// MAGIC 
// MAGIC ## Instructions
// MAGIC * Insert solutions wherever it says `FILL_IN`
// MAGIC * Feel free to copy/paste code from the previous notebook, where applicable
// MAGIC * Run test cells to verify that your solution is correct
// MAGIC 
// MAGIC ## Prerequisites
// MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
// MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
// MAGIC * Databricks Runtime 4.2 or greater
// MAGIC * Completed courses Spark-SQL, DataFrames or ETL-Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>, or have similar knowledge

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Getting Started</h2>
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise 1: Read data into a stream</h2>
// MAGIC 
// MAGIC The dataset used in this exercise consists of flight information about flights from/to various airports in 2007.
// MAGIC 
// MAGIC You have already seen this dataset in Exercise 1 of Notebook 02.
// MAGIC 
// MAGIC To refresh your memory, take a look at the first few lines of the dataset.

// COMMAND ----------

display(
  spark.read.parquet("dbfs:/mnt/training/asa/flights/2007-01-stream.parquet/part-00000-tid-9167815511861375854-22d81a30-d5b4-43d0-9216-0c20d14c3f54-178-c000.snappy.parquet")
)

// COMMAND ----------

// MAGIC %md
// MAGIC For this exercise you will need to complete the following tasks:
// MAGIC 0. Start a stream that reads parquet files dumped to the directory `dataPath`
// MAGIC 0. Control the size of each partition by forcing Spark to processes only 1 file per trigger.
// MAGIC 
// MAGIC Other notes:
// MAGIC 0. The source data has already been defined as `dataPath`
// MAGIC 0. The schema has already be defined as `parquetSchema`

// COMMAND ----------

// ANSWER
lazy val dataPath = "/mnt/training/asa/flights/2007-01-stream.parquet/"

lazy val parquetSchema = "DepartureAt timestamp, FlightDate string, DepTime string, CRSDepTime string, ArrTime string, CRSArrTime string, UniqueCarrier string, FlightNum integer, TailNum string, ActualElapsedTime string, CRSElapsedTime string, AirTime string, ArrDelay string, DepDelay string, Origin string, Dest string, Distance string, TaxiIn string, TaxiOut string, Cancelled integer, CancellationCode string, Diverted integer, CarrierDelay string, WeatherDelay string, NASDelay string, SecurityDelay string, LateAircraftDelay string"

// Configure the shuffle partitions to match the number of cores
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

val streamDF = spark                // Start with the SparkSesion
  .readStream                       // Get the DataStreamReader
  .format("parquet")                // Configure the stream's source for the appropriate file type
  .schema(parquetSchema)            // Specify the parquet files' schema
  .option("maxFilesPerTrigger", 1)  // Restrict Spark to processing only 1 file per trigger
  .load(dataPath)                   // Load the parquet data specifying its location as dataPath

// COMMAND ----------

// TEST - Run this cell to test your solution.
lazy val schemaStr = streamDF.schema.mkString("")

dbTest("SS-03-shuffles",  sc.defaultParallelism, spark.conf.get("spark.sql.shuffle.partitions").toInt)

dbTest("SS-03-schema-1",  true,  schemaStr.contains("(DepartureAt,TimestampType,true)"))
dbTest("SS-03-schema-2",  true,  schemaStr.contains("(FlightDate,StringType,true)"))
dbTest("SS-03-schema-3",  true,  schemaStr.contains("(DepTime,StringType,true)"))
dbTest("SS-03-schema-4",  true,  schemaStr.contains("(CRSDepTime,StringType,true)"))
dbTest("SS-03-schema-5",  true,  schemaStr.contains("(ArrTime,StringType,true)"))
dbTest("SS-03-schema-6",  true,  schemaStr.contains("(CRSArrTime,StringType,true)"))
dbTest("SS-03-schema-7",  true,  schemaStr.contains("(UniqueCarrier,StringType,true)"))
dbTest("SS-03-schema-8",  true,  schemaStr.contains("(FlightNum,IntegerType,true)"))
dbTest("SS-03-schema-9",  true,  schemaStr.contains("(TailNum,StringType,true)"))
dbTest("SS-03-schema-10",  true,  schemaStr.contains("(ActualElapsedTime,StringType,true)"))
dbTest("SS-03-schema-11",  true,  schemaStr.contains("(CRSElapsedTime,StringType,true)"))
dbTest("SS-03-schema-12",  true,  schemaStr.contains("(AirTime,StringType,true)"))
dbTest("SS-03-schema-13",  true,  schemaStr.contains("(ArrDelay,StringType,true)"))
dbTest("SS-03-schema-14",  true,  schemaStr.contains("(DepDelay,StringType,true)"))
dbTest("SS-03-schema-15",  true,  schemaStr.contains("(Origin,StringType,true)"))
dbTest("SS-03-schema-16",  true,  schemaStr.contains("(Dest,StringType,true)"))
dbTest("SS-03-schema-17",  true,  schemaStr.contains("(Distance,StringType,true)"))
dbTest("SS-03-schema-18",  true,  schemaStr.contains("(TaxiIn,StringType,true)"))
dbTest("SS-03-schema-19",  true,  schemaStr.contains("(TaxiOut,StringType,true)"))
dbTest("SS-03-schema-20",  true,  schemaStr.contains("(Cancelled,IntegerType,true)"))
dbTest("SS-03-schema-21",  true,  schemaStr.contains("(CancellationCode,StringType,true)"))
dbTest("SS-03-schema-22",  true,  schemaStr.contains("(Diverted,IntegerType,true)"))
dbTest("SS-03-schema-23",  true,  schemaStr.contains("(CarrierDelay,StringType,true)"))
dbTest("SS-03-schema-24",  true,  schemaStr.contains("(WeatherDelay,StringType,true)"))
dbTest("SS-03-schema-25",  true,  schemaStr.contains("(NASDelay,StringType,true)" ))
dbTest("SS-03-schema-26",  true,  schemaStr.contains("(SecurityDelay,StringType,true)"))
dbTest("SS-03-schema-27",  true,  schemaStr.contains("(LateAircraftDelay,StringType,true)"))

println("Tests passed!")
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise 2: Plot grouped events</h2>
// MAGIC 
// MAGIC Plot the count of all flights aggregated by a 30 minute window and `UniqueCarrier`. 
// MAGIC 
// MAGIC Ignore any events delayed by 300 minutes or more.
// MAGIC 
// MAGIC You will need to:
// MAGIC 0. Use a watermark to discard events not received within 300 minutes
// MAGIC 0. Configure the stream for a 30 minute sliding window
// MAGIC 0. Aggregate by the 30 minute window and the column `UniqueCarrier`
// MAGIC 0. Add the column `start` by extracting it from `window.start`
// MAGIC 0. Sort the stream by `start`
// MAGIC 
// MAGIC In order to create a LIVE bar chart of the data, you'll need to specify the following <b>Plot Options</b>:
// MAGIC * **Keys** is set to `start`
// MAGIC * **Series groupings** is set to `UniqueCarrier`
// MAGIC * **Values** is set to `count`

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions.{hour, window, col}

val countsDF = streamDF                                           // Start with the DataFrame
  .withWatermark("DepartureAt", "300 minutes")                    // Specify the watermark
  .groupBy(window($"DepartureAt", "30 minute"), $"UniqueCarrier") // Aggregate the data
  .count()                                                        // Produce a count for each aggreate
  .withColumn("start", $"window.start")                           // Add the column "start", extracting it from "window.start"
  .orderBy($"start")                                              // Sort the stream by "start" 

display(countsDF)

// COMMAND ----------

// TEST     - Run this cell to test your solution.
lazy val schemaStr = countsDF.schema.mkString("")

dbTest("SS-03-schema-1", true, schemaStr.contains("(UniqueCarrier,StringType,true)"))
dbTest("SS-03-schema-2", true, schemaStr.contains("(count,LongType,false)"))
dbTest("SS-03-schema-3", true, schemaStr.contains("(start,TimestampType,true)"))

println("Tests passed!")
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC Wait until stream is done initializing...

// COMMAND ----------

untilStreamIsReady("delta_1s")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise 3: Stop streaming jobs</h2>
// MAGIC 
// MAGIC Before we can conclude, we need to shut down all active streams.

// COMMAND ----------

// ANSWER
for (s <- spark.streams.active) // Iterate over all the active streams
  s.stop()                      // Stop the stream

// COMMAND ----------

// TEST - Run this cell to test your solution.
dbTest("SS-03-numActiveStreams", 0, spark.streams.active.length)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Next Steps</h2>
// MAGIC 
// MAGIC Start the next lesson, [Using Kafka]($../SS 04 - Using Kafka).

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>