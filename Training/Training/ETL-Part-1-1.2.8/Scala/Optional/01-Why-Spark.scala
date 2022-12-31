// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Why Apache Spark?
// MAGIC 
// MAGIC Identify the problems Apache Spark&trade; and Databricks&reg; are well suited to solve.
// MAGIC 
// MAGIC ## In this lesson you
// MAGIC * Identify the types of tasks well suited to Apache Spark’s Unified Analytics Engine.
// MAGIC * Identify examples of tasks not well suited for Apache Spark.
// MAGIC 
// MAGIC ## Audience
// MAGIC * Primary Audience: Data Analysts
// MAGIC * Additional Audiences: Data Engineers and Data Scientists
// MAGIC 
// MAGIC ## Prerequisites
// MAGIC * Web browser: Chrome or Firefox
// MAGIC * Lesson: [Getting Started]($./01-Getting-Started)
// MAGIC * Concept: <a href="https://www.w3schools.com/sql" target="_blank">Basic SQL</a>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The command `%run` runs another notebook (in this case `Classroom-Setup`), which prepares the data for this lesson.

// COMMAND ----------

// MAGIC %run "../Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %run "../Includes/Stream-Generator"

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Lesson
// MAGIC 
// MAGIC ### Use cases for Apache Spark
// MAGIC * Read and process huge files and data sets
// MAGIC * Query, explore, and visualize data sets
// MAGIC * Join disparate data sets found in data lakes
// MAGIC * Train and evaluate machine learning models
// MAGIC * Process live streams of data
// MAGIC * Perform analysis on large graph data sets and social networks
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Focus on learning the types of problems solved by Spark; the code examples are explained either later in this course or future courses.

// COMMAND ----------

// MAGIC %md
// MAGIC Watch the following video:
// MAGIC 
// MAGIC <iframe  
// MAGIC src="//fast.wistia.net/embed/iframe/5ir9qvipzn?videoFoam=true"
// MAGIC style="border:1px solid #1cb1c2;"
// MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
// MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
// MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
// MAGIC <div>
// MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/5ir9qvipzn?seo=false">
// MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div>
// MAGIC   <div>**Apache Spark is used to...**</div>
// MAGIC   <div><h3 style="margin-top:0; margin-bottom:0.75em">Read and process huge files and data sets</h3></div>
// MAGIC </div>
// MAGIC Spark provides a query engine capable of processing data in very, very large data files.  Some of the largest Spark jobs in the world run on Petabytes of data.

// COMMAND ----------

// MAGIC %md
// MAGIC The files in `dbfs:/mnt/training/asa/flights/all-by-year/` are stored in Amazon's S3 (Simple Storage Service) and made easily accessible using the Databricks Filesystem (dbfs).
// MAGIC 
// MAGIC The `%fs ls` command lists the contents of the bucket.  There are 22 comma-separated-values (CSV) files containing flight data for 1987-2008.  Spark can readily handle petabytes of data given a sufficiently large cluster.

// COMMAND ----------

// MAGIC %fs ls dbfs:/mnt/training/asa/flights/all-by-year/

// COMMAND ----------

// MAGIC %md
// MAGIC The `CREATE TABLE` statement below registers the CSV file as a SQL Table.  The CSV file can then be queried directly using SQL.
// MAGIC 
// MAGIC In order to allow this example to run quickly on a small cluster, we'll use the file `small.csv` instead.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE DATABASE IF NOT EXISTS Databricks;
// MAGIC USE Databricks;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS AirlineFlight
// MAGIC USING CSV
// MAGIC OPTIONS (
// MAGIC   header="true",
// MAGIC   delimiter=",",
// MAGIC   inferSchema="true",
// MAGIC   path="dbfs:/mnt/training/asa/flights/small.csv"
// MAGIC );
// MAGIC 
// MAGIC CACHE TABLE AirlineFlight;
// MAGIC 
// MAGIC SELECT * FROM AirlineFlight;

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div>
// MAGIC   <div>**Apache Spark is used to...**</div>
// MAGIC   <div><h3 style="margin-top:0; margin-bottom:0.75em">Query, explore, and visualize data sets</h3></div>
// MAGIC </div>
// MAGIC 
// MAGIC Spark can perform complex queries to extract insights from large files and visualize the results.

// COMMAND ----------

// MAGIC %md
// MAGIC The example below creates a table from a CSV file listing flight delays by airplane, counts the number of delays per model of airplane, and then graphs it.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS AirlinePlane
// MAGIC USING csv
// MAGIC OPTIONS (
// MAGIC   header = "true",
// MAGIC   delimiter = ",",
// MAGIC   inferSchema = "false",
// MAGIC   path = "dbfs:/mnt/training/asa/planes/plane-data.csv"
// MAGIC );
// MAGIC 
// MAGIC CACHE TABLE AirlinePlane;
// MAGIC 
// MAGIC SELECT Model, count(*) AS Delays FROM AirlinePlane WHERE Model IS NOT NULL GROUP BY Model ORDER BY Delays DESC LIMIT 10;

// COMMAND ----------

// MAGIC %md
// MAGIC To visualize the results results above, click on the plot button directly below the results above, then click the Plot Options button set:
// MAGIC * Display Type: Bar Chart
// MAGIC * Keys: (Empty)
// MAGIC * Series Groupings: Model
// MAGIC 
// MAGIC And resize the plot to be taller and wider by dragging the triangle in the bottom-right of the plot.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div>
// MAGIC   <div>**Apache Spark is used to...**</div>
// MAGIC   <div><h3 style="margin-top:0; margin-bottom:0.75em">Join disparate data sets found in data lakes</h3></div>
// MAGIC </div>
// MAGIC 
// MAGIC Companies frequently have thousands of large data files gathered from various teams and departments, typically using a diverse variety of formats, including CSV, JSON and XML.
// MAGIC 
// MAGIC These are called Data Lakes. Data Lakes differ from Data Warehouses in that they don't require someone to spend weeks or months preparing a unified enterprise schema and then populating it.
// MAGIC 
// MAGIC Frequently an analyst wishes to run simple queries across various data files, without taking the time required to construct a fully-fledged Data Warehouse.
// MAGIC 
// MAGIC Spark excels in this type of workload by enabling users to simultaneously query files from many different storage locations, and then formats and join them together using Spark SQL.
// MAGIC 
// MAGIC Spark can later load the data into a Data Warehouse if desired.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT p.manufacturer AS Manufacturer,
// MAGIC        avg(depDelay) AS Delay
// MAGIC FROM AirlinePlane p
// MAGIC JOIN AirlineFlight f ON p.tailnum = f.tailnum
// MAGIC WHERE p.manufacturer IS NOT null
// MAGIC GROUP BY p.manufacturer
// MAGIC ORDER BY Delay DESC
// MAGIC LIMIT 10

// COMMAND ----------

// MAGIC %md
// MAGIC To visualize the results results above, click on the plot button directly below the results above, then click the Plot Options button set:
// MAGIC * Display Type: Pie Chart
// MAGIC * Donut: Unchecked
// MAGIC 
// MAGIC And resize the plot to be taller and wider by dragging the triangle in the bottom-right of the plot.

// COMMAND ----------

// MAGIC %md
// MAGIC Not only does Spark bring together files from many different locations, it also brings in disparate data sources and file types such as:
// MAGIC * JDBC Data Sources like SQL Server, Azure SQL Database, MySQL, PostgreSQL, Oracle,  etc.
// MAGIC * Parquet files
// MAGIC * CSV files
// MAGIC * ORC files
// MAGIC * JSON files
// MAGIC * HDFS file systems
// MAGIC * Apache Kafka
// MAGIC * And with a little extra work, Web Services Endpoints, TCP-IP sockets, and just about anything else you can imagine!

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div>
// MAGIC   <div>**Apache Spark is used to...**</div>
// MAGIC   <div><h3 style="margin-top:0; margin-bottom:0.75em">Train and evaluate machine learning models</h3></div>
// MAGIC </div>
// MAGIC 
// MAGIC Spark performs predictive analytics using machine learning algorithms.
// MAGIC 
// MAGIC The example below trains a linear regression model using past flight data to predict delays based on the hour of the day.

// COMMAND ----------

import org.apache.spark.sql.functions.{floor, translate, round}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression

val inputDF = spark.read.table("AirlineFlight")
  .withColumn("HourOfDay", floor($"CRSDepTime"/100))
  .withColumn("DepDelay", translate($"DepDelay", "NA", "0").cast("integer"))

val Array(trainingDF, testDF) = inputDF.randomSplit(Array(0.80, 0.20), 999)

val pipeline = new Pipeline().setStages(Array(
  new OneHotEncoder()
    .setInputCol("HourOfDay")
    .setOutputCol("HourVector"),
  new VectorAssembler()
    .setInputCols(Array("HourVector"))
    .setOutputCol("Features"),
  new LinearRegression()
    .setFeaturesCol("Features")
    .setLabelCol("DepDelay")
    .setPredictionCol("DepDelayPredicted")
    .setRegParam(0.0)))

val model = pipeline.fit(trainingDF)
val resultDF = model.transform(testDF)

val displayDF = resultDF.select($"Year", $"Month", $"DayOfMonth", $"CRSDepTime", $"UniqueCarrier", $"FlightNum", $"DepDelay", round($"DepDelayPredicted", 2).as("DepDelayPredicted"));
display(displayDF)

// COMMAND ----------

display(
  resultDF
    .groupBy("HourOfDay")
    .avg("DepDelay", "DepDelayPredicted")
    .toDF("HourOfDay", "Actual", "Predicted")
    .orderBy("HourOfDay")
)

// COMMAND ----------

// MAGIC %md
// MAGIC To visualize the results results above, click on the plot button directly below the results above, then click the Plot Options button set:
// MAGIC * Display Type: Line Chart
// MAGIC * Keys: HourOfDay
// MAGIC * Values: Actual, Predicted
// MAGIC 
// MAGIC And resize the plot to be taller and wider by dragging the triangle in the bottom-right of the plot.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div>
// MAGIC   <div>**Apache Spark is used to...**</div>
// MAGIC   <div><h3 style="margin-top:0; margin-bottom:0.75em">Process live streams of data</h3></div>
// MAGIC </div>
// MAGIC 
// MAGIC Besides aggregating static data sets, Spark can also process live streams of data such as:
// MAGIC * File Streams
// MAGIC * TCP-IP Streams
// MAGIC * Apache Kafka
// MAGIC * Custom Streams like Twitter & Facebook

// COMMAND ----------

// MAGIC %md
// MAGIC Before processing streaming data, a data source is required.
// MAGIC 
// MAGIC The cell below first deletes any temp files, and then generates a stream of fake flight data for up to 30 minutes.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC // Clean any temp files from previous runs.
// MAGIC DummyDataGenerator.clean()
// MAGIC 
// MAGIC // Generate data for 5 minutes.
// MAGIC // To force it to stop rerun with 0.
// MAGIC DummyDataGenerator.start(5)

// COMMAND ----------

// MAGIC %md
// MAGIC The example below connects to and processes a fake, fire-hose of flight data by:
// MAGIC 0. Reading in a stream of constantly updating CSV files.
// MAGIC 0. Parsing the flight's date and time.
// MAGIC 0. Computing the average delay for each airline based on the most recent 15 seconds of flight data.
// MAGIC 0. Plotting the results in near-real-time.
// MAGIC 
// MAGIC **Disclaimer:** The real-time data represented here is completely fictional and is not intended to reflect actual airline performance.

// COMMAND ----------

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.{date_format, unix_timestamp, window}

spark.conf.set("spark.sql.shuffle.partitions", "8")

val flightSchema = new StructType()
  .add("FlightNumber", "integer")
  .add("DepartureTime", "string")
  .add("Delay", "double")
  .add("Airline", "string")

val streamingDF = spark.readStream
  .schema(flightSchema)
  .csv(DummyDataGenerator.streamDirectory)
  .withColumn("DepartureTime", unix_timestamp($"DepartureTime", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp"))
  .withWatermark("DepartureTime", "5 minute")
  .groupBy( window($"DepartureTime", "15 seconds"), $"Airline" )
  .avg("Delay")
  .select($"window.start".as("Start"), $"Airline", $"avg(delay)".as("Average Delay"))
  .orderBy($"Start", $"Airline")
  .select(date_format($"Start", "HH:mm:ss").as("Time"), $"Airline", $"Average Delay")

display(streamingDF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Remember to stop your stream by clicking the **Cancel** link up above.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div>
// MAGIC   <div>**Apache Spark is used to...**</div>
// MAGIC   <div><h3 style="margin-top:0; margin-bottom:0.75em">Perform analysis on large graph data sets and social networks</h3></div>
// MAGIC </div>
// MAGIC 
// MAGIC The open source <a href="https://graphframes.github.io/" target="_blank">GraphFrames</a> library extends Spark to study not the data itself, but the network of relationships between entities.  This facilitates queries such as:
// MAGIC * **Shortest Path:** What is the shortest route from Springfield, IL to Austin, TX?
// MAGIC * **Page Rank:** Which airports are the most important hubs in the USA?
// MAGIC * **Connected Components:** Find strongly connected groups of friends on Facebook.
// MAGIC * (just to name a few)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Connected Graphs
// MAGIC 
// MAGIC The example below is a visualization of a network of airports connected by flight routes.
// MAGIC 
// MAGIC Databricks can display this network using popular third-party visualization library such as:
// MAGIC * <a href="https://d3js.org/" target="_blank">D3.js - Data-Driven Documents</a>
// MAGIC * <a href="https://matplotlib.org/" target="_blank">Matplotlib: Python plotting</a>
// MAGIC * <a href="http://ggplot.yhathq.com/" target="_blank">ggplot</a>
// MAGIC * <a href="https://plot.ly/" target="_blank">Plotly<a/>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <iframe style='border-style:none; position:absolute; left:-150px; width:1170px; height:700px'
// MAGIC           src='https://mbostock.github.io/d3/talk/20111116/#14'
// MAGIC />

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC #### PageRank algorithm
// MAGIC The <a href="https://en.wikipedia.org/wiki/PageRank" target="_blank">PageRank</a> algorithm, named after Google co-founder Larry Page, assesses the importance of a hub in a network.
// MAGIC 
// MAGIC The example below uses the <a href="https://graphframes.github.io/" target="_blank">GraphFrames</a> API  to compute the **PageRank** of each airport in the United States and shows the top 10 most important US airports.
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> This example requires the GraphFrames library that may not have been setup on your cluster.  Read the example below rather than running it.

// COMMAND ----------

import org.apache.spark.sql.functions.{concat_ws, round}
import org.graphframes.GraphFrame

val flightVerticesDF = spark.read
  .option("header", true)
  .option("delimiter", "\t")
  .csv("dbfs:/mnt/training/asa/airport-codes/airport-codes.txt")
  .withColumnRenamed("IATA", "id")

val flightEdgesDF = spark.table("AirlineFlight")
  .withColumnRenamed("Origin", "src")
  .withColumnRenamed("Dest", "dst")

val flightGF = GraphFrame(flightVerticesDF, flightEdgesDF)
val pageRankDF = flightGF.pageRank.tol(0.05).run()
val resultsDF = pageRankDF.vertices
  .select(concat_ws(", ", $"city", $"state").as("Location"),
          round($"pagerank", 1).as("Rank"))
  .orderBy($"pagerank".desc)

display(resultsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Review
// MAGIC **Question:** Which of the following are good applications for Apache Spark? (Select all that apply.)
// MAGIC 0. Querying, exploring, and analyzing very large files and data sets
// MAGIC 0. Joining data lakes
// MAGIC 0. Machine learning and predictive analytics
// MAGIC 0. Processing streaming data
// MAGIC 0. Graph analytics
// MAGIC 0. Overnight batch processing of very large files
// MAGIC 0. Updating individual records in a database
// MAGIC 
// MAGIC **Answer:** All but #7. Apache Spark uses SQL to read and performs analysis on large files, but it is not a Database.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Topics & Resources
// MAGIC 
// MAGIC **Q:** What makes Spark different than Hadoop?  
// MAGIC **A:** Spark on Databricks performs 10-2000x faster than Hadoop Map-Reduce.  It does this by providing a high-level query API which allows Spark to highly optimize the internal execution without adding complexity for the user.  Internally, Spark employs a large number of optimizations such as pipelining related tasks together into a single operation, communicating in memory, using just-in-time code generation, query optimization, efficient tabular memory (Tungsten), caching, and more.
// MAGIC 
// MAGIC **Q:** What are the visualization options in Databricks?  
// MAGIC **A:** Databricks provides a wide variety of <a href="https://docs.databricks.com/user-guide/visualizations/index.html" target="_blank">built-in visualizations</a>.  Databricks also supports a variety of 3rd party visualization libraries, including <a href="https://d3js.org/" target="_blank">d3.js</a>, <a href="https://matplotlib.org/" target="_blank">matplotlib</a>, <a href="http://ggplot.yhathq.com/" target="_blank">ggplot</a>, and <a href="https://plot.ly/" target="_blank">plotly<a/>.
// MAGIC 
// MAGIC **Q:** Where can I learn more about DBFS?  
// MAGIC **A:** See the document <a href="https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html" target="_blank">Databricks File System - DBFS</a>.
// MAGIC 
// MAGIC **Q:** Where can I find a list of the machine learning algorithms supported by Spark?  
// MAGIC **A:** The Spark documentation for Machine Learning describes the algorithms for classification, regression, clustering, recommendations (ALS), neural networks, and more.  The documentation doesn't provide a single consolidated list, but by browsing through the <a href="http://spark.apache.org/docs/latest/ml-guide.html" target="_blank">Spark MLLib documentation</a> you can find the supported algorithms.  Additionally, <a href="https://spark-packages.org/" target="_blank">3rd party libraries</a> provide even more algorithms and capabilities.
// MAGIC 
// MAGIC **Q:** Where can I learn more about stream processing in Spark?  
// MAGIC **A:** See the <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html" target="_blank">Structured Streaming Programming Guide</a>.
// MAGIC 
// MAGIC **Q:** Where can I learn more about GraphFrames?  
// MAGIC **A:** See the <a href="http://graphframes.github.io/" target="_blank">GraphFrames Overview</a>.  The Databricks blog has an <a href="https://databricks.com/blog/2016/03/16/on-time-flight-performance-with-graphframes-for-apache-spark.html">example</a> which uses d3 to perform visualizations of GraphFrame data.
// MAGIC 
// MAGIC **Q:** How do I upload files to Amazon S3?  
// MAGIC **A:** Amazon S3 is a service provided by Amazon, and each company has an Amazon account with company-controlled access rules.  File uploads can be done through the Amazon Web Services API, website, or command-line interface.  Databricks also provides a table upload API which is discussed in the [next lesson]($./03-Accessing-Data).

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>