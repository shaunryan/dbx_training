# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Gain Actionable Insights from Twitter Data
# MAGIC 
# MAGIC In this capstone project, you will use Structured Streaming to gain insight from streaming Twitter data.
# MAGIC 
# MAGIC The executive team would like to have access to some key business metrics such as
# MAGIC * most tweeted hashtag in last 5 minute window
# MAGIC * a map of where tweets are coming from
# MAGIC 
# MAGIC ## Instructions
# MAGIC * Insert solutions wherever it says `FILL_IN`
# MAGIC * Feel free to copy/paste code from the previous notebooks, where applicable
# MAGIC * Run test cells to verify that your solution is correct
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers
# MAGIC * Additional Audiences: Data Analysts and Data Scientists
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
# MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
# MAGIC * Databricks Runtime 4.2 or greater
# MAGIC * Completed courses Spark-SQL, DataFrames or ETL-Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>, or have similar knowledge
# MAGIC * Have done the rest of this course

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Getting Started</h2>
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Step 1</h2>
# MAGIC <h3>Read Streaming Data from Input Source</h3>
# MAGIC 
# MAGIC The input source is a a Kafka feed of Twitter data
# MAGIC 
# MAGIC For this step you will need to:
# MAGIC 0. Use the `format()` operation to specify "kafka" as the type of the stream
# MAGIC 0. Specify the location of the Kafka server by setting the option "kafka.bootstrap.servers" with one of the following values (depending on where you are located): 
# MAGIC  * **server1.databricks.training:9092** (US-Oregon)
# MAGIC  * **server2.databricks.training:9092** (Singapore)
# MAGIC 0. Indicate which topics to listen to by setting the option "subscribe" to "tweets"
# MAGIC 0. Throttle Kafka's processing of the streams
# MAGIC 0. Rewind stream to beginning when we restart notebook
# MAGIC 0. Load the input data stream in as a DataFrame
# MAGIC 0. Select the column `value` - cast it to a `STRING`

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

kafkaServer = "server1.databricks.training:9092"   # US (Oregon)
# kafkaServer = "server2.databricks.training:9092" # Singapore

rawDF = (spark.readStream
  .format("kafka")                                       # Specify "kafka" as the type of the stream
  .option("kafka.bootstrap.servers", kafkaServer)        # Set the location of the kafka server
  .option("subscribe", "tweets")                         # Indicate which topics to listen to
  .option("maxOffsetsPerTrigger", 1000)                  # Throttle Kafka's processing of the streams
  .option("startingOffsets", "earliest")                 # Rewind stream to beginning when we restart notebook
  .load()                                                # Load the input data stream in as a DataFrame
  .select(col("value").cast("STRING"))                   # Select the "value" column and cast to a string
)

# COMMAND ----------

# TEST - Run this cell to test your solution.
schemaStr = str(rawDF.schema)

dbTest("SS-06-schema-value",     True, "(value,StringType,true)" in schemaStr)
dbTest("SS-06-is-streaming",     True, rawDF.isStreaming)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Step 2</h2>
# MAGIC <h3>A Schema for parsing JSON</h3>
# MAGIC 
# MAGIC Becase the schema is so complex, it is being provided for you.
# MAGIC 
# MAGIC Simply run the following cell and proceed to the next step.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ArrayType

twitSchema = StructType([
  StructField("hashTags", ArrayType(StringType(), False), True),
  StructField("text", StringType(), True),   
  StructField("userScreenName", StringType(), True),
  StructField("id", LongType(), True),
  StructField("createdAt", LongType(), True),
  StructField("retweetCount", IntegerType(), True),
  StructField("lang", StringType(), True),
  StructField("favoriteCount", IntegerType(), True),
  StructField("user", StringType(), True),
  StructField("place", StructType([
    StructField("coordinates", StringType(), True), 
    StructField("name", StringType(), True),
    StructField("placeType", StringType(), True),
    StructField("fullName", StringType(), True),
    StructField("countryCode", StringType(), True)]), 
  True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Step 3</h2>
# MAGIC <h3>Create a JSON DataFrame</h3>
# MAGIC 
# MAGIC From the `rawDF` parse out the json subfields using `from_json`. Create a DataFrame that has fields
# MAGIC * `time`
# MAGIC * `json`, a nested field that has all the rest of the data
# MAGIC * promote all `json` subfields to fields.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import from_json, expr, col

cleanDF = (rawDF
  .withColumn("json", from_json(col("value"), twitSchema))                      # Add the column "json" by parsing the column "value" with "from_json"
  .select(
    expr("cast(cast(json.createdAt as double)/1000 as timestamp) as time"),     # Cast "createdAt" column properly, call it "time"
    col("json.hashTags").alias("hashTags"),                                     # Promote subfields of "json" column e.g. "json.field" to "field"
    col("json.text").alias("text"),                                             # Repeat for each subfields of "json"
    col("json.userScreenName").alias("userScreenName"),
    col("json.id").alias("id"), 
    col("json.retweetCount").alias("retweetCount"),
    col("json.lang").alias("lang"),
    col("json.favoriteCount").alias("favoriteCount"),
    col("json.user").alias("user"),
    col("json.place.coordinates").alias("coordinates"),
    col("json.place.name").alias("name"),
    col("json.place.placeType").alias("placeType"),
    col("json.place.fullName").alias("fullName"),
    col("json.place.countryCode").alias("countryCode")   
  )
)

# COMMAND ----------

# TEST - Run this cell to test your solution.
schemaStr = str(cleanDF.schema)

dbTest("SS-06-schema-hashTag",  True, "hashTags,ArrayType(StringType,true)" in schemaStr)
dbTest("SS-06-schema-text",  True, "(text,StringType,true)" in schemaStr)
dbTest("SS-06-schema-userScreenName",  True, "(userScreenName,StringType,true)" in schemaStr)
dbTest("SS-06-schema-id",  True, "(id,LongType,true)" in schemaStr)
dbTest("SS-06-schema-time",  True, "(time,TimestampType,true)" in schemaStr)
dbTest("SS-06-schema-retweetCount",  True, "(retweetCount,IntegerType,true)" in schemaStr)
dbTest("SS-06-schema-lang",  True, "(lang,StringType,true)" in schemaStr)
dbTest("SS-06-schema-favoriteCount",  True, "(favoriteCount,IntegerType,true)" in schemaStr)
dbTest("SS-06-schema-user",  True, "(user,StringType,true)" in schemaStr)
dbTest("SS-06-schema-coordinates",  True, "(coordinates,StringType,true)" in schemaStr)
dbTest("SS-06-schema-name",  True, "(name,StringType,true)" in schemaStr)
dbTest("SS-06-schema-placeType",  True, "(placeType,StringType,true)" in schemaStr)
dbTest("SS-06-schema-fullName",  True, "(fullName,StringType,true)" in schemaStr)
dbTest("SS-06-schema-countryCode",  True, "(countryCode,StringType,true)" in schemaStr)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Step 4</h2>
# MAGIC <h3>Display Twitter Data as a Table</h3>
# MAGIC 
# MAGIC Click the left-most button in the bottom left corner.

# COMMAND ----------

# ANSWER
display(cleanDF)

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("SS-06-numActiveStreams", True, len(spark.streams.active) > 0)
       
print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC When you are done, stop the stream:

# COMMAND ----------

# ANSWER
for streamingQuery in spark.streams.active:
  streamingQuery.stop()

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("SS-06-numActiveStreams1", 0, len(spark.streams.active))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Step 5</h2>
# MAGIC <h3>Hashtag Processing</h3>
# MAGIC 
# MAGIC In this exercise, we do ETL processing on the `hashTags` column.
# MAGIC 
# MAGIC The goal is to first convert hash tags all to lower case then group tweets and count by hash tags.
# MAGIC 
# MAGIC You will notice that `hashTags` is an array of hash tags, which you will have to break up (use `explode` function).
# MAGIC 
# MAGIC The `explode` method allows you to split an array column into multiple rows, copying all the other columns into each new row.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import explode, lower

twitCountsDF = (cleanDF                          # Start with "cleanDF"
  .withColumn("hashTag", explode("hashTags"))    # Explode the array "hashTags" into "hashTag"
  .withColumn("hashTag", lower(col("hashTag")))  # Convert "hashTag" to lower case
  .groupBy("hashTag")                            # Aggregate by "hashTag"
  .count()                                       # For the aggregate, produce a count  
  .orderBy(col("count").desc())                  # Sort by "count"
  .limit(25)                                     # Limit the result to 25 records
)

# COMMAND ----------

# TEST - Run this cell to test your solution.
schemaStr = str(twitCountsDF.schema)

dbTest("SS-06-schema-hashTag", True, "(hashTag,StringType,true)" in schemaStr)
dbTest("SS-06-schema-count",   True, "(count,LongType,false)" in schemaStr)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Step 6</h2>
# MAGIC <h3>Plot Counts of Top 25 Most Popular Hashtags</h3>
# MAGIC 
# MAGIC Under <b>Plot Options</b>, use the following:
# MAGIC * <b>Keys:</b> `hashTag`
# MAGIC * <b>Values:</b> `count`
# MAGIC 
# MAGIC In <b>Display type</b>, use <b>Pie Chart</b> and click <b>Apply</b>.
# MAGIC 
# MAGIC Once you apply the plot options, be prepared to increase the size of the plot graphic using the resize widget in the lower right corner of the graphic area. 

# COMMAND ----------

# ANSWER
display(twitCountsDF)

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("SS-06-numActiveStreams", True, len(spark.streams.active) > 0)
       
print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC When you are done, stop the stream:

# COMMAND ----------

for streamingQuery in spark.streams.active:
  streamingQuery.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Step 7</h2>
# MAGIC <h3>Read in File with Two Letter to Three Letter Country Codes</h3>
# MAGIC 
# MAGIC For this next part we are going to take a look at the number of requests per country.
# MAGIC 
# MAGIC To get started, we first need a lookup table that will give us the 3-character country code.
# MAGIC 
# MAGIC 0. Read in the file at `/mnt/training/countries/ISOCountryCodes/ISOCountryLookup.parquet`
# MAGIC 0. We will be interested in the `alpha2Code` and `alpha3Code` fields later

# COMMAND ----------

# ANSWER
countryCodeDF = spark.read.parquet("/mnt/training/countries/ISOCountryCodes/ISOCountryLookup.parquet")

# COMMAND ----------

# TEST - Run this cell to test your solution.
schemaStr = str(countryCodeDF.schema)

dbTest("SS-06-schema-1", True, "(EnglishShortName,StringType,true)" in schemaStr)
dbTest("SS-06-schema-2", True, "(alpha2Code,StringType,true)" in schemaStr)
dbTest("SS-06-schema-3", True, "(alpha3Code,StringType,true)" in schemaStr)
dbTest("SS-06-schema-4", True, "(numericCode,StringType,true)" in schemaStr)
dbTest("SS-06-schema-5", True, "(ISO31662SubdivisionCode,StringType,true)" in schemaStr)
dbTest("SS-06-schema-6", True, "(independentTerritory,StringType,true)" in schemaStr)

dbTest("SS-06-streaming-7", False, countryCodeDF.isStreaming)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Step 8</h2>
# MAGIC <h3>Join Tables &amp; Aggregate By Country</h3>
# MAGIC 
# MAGIC In `cleanDF`, there is a `countryCode` field. However, it is in the form of a two-letter country code.
# MAGIC 
# MAGIC The `display` map expects a three-letter country code.
# MAGIC 
# MAGIC In order to retrieve tweets with three-letter country codes, we will have to join `cleanDF` with `countryCodesDF`.

# COMMAND ----------

# ANSWER
mappedDF = (cleanDF
  .filter(col("countryCode").isNotNull())                                      # Filter out any nulls for "countryCode"
  .join(countryCodeDF, cleanDF["countryCode"] == countryCodeDF["alpha2Code"])  # Join the two tables on "countryCode" and "alpha2Code"
  .groupBy(col("alpha3Code"))                                                  # Aggregate by country, "alpha3Code"
  .count()                                                                     # Produce a count of each aggregate
)

# COMMAND ----------

# TEST - Run this cell to test your solution.
schemaStr = str(mappedDF.schema)
print(schemaStr)

dbTest("SS-06-schema-1",  True, "alpha3Code,StringType,true" in schemaStr)
dbTest("SS-06-schema-2",  True, "count,LongType,false" in schemaStr)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Step 9</h2>
# MAGIC <h3>Plot Tweet Counts on a World Map</h3>
# MAGIC 
# MAGIC Under <b>Plot Options</b>, use the following:
# MAGIC * <b>Keys:</b> `alpha3Code`
# MAGIC * <b>Values:</b> `count`
# MAGIC 
# MAGIC In <b>Display type</b>, use <b>World map</b> and click <b>Apply</b>.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/Structured-Streaming/plot-options-map-06.png"/>

# COMMAND ----------

# ANSWER
display(mappedDF)

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("SS-06-numActiveStreams", True, len(spark.streams.active) > 0)
       
print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Step 10: Write Stream</h2>
# MAGIC 
# MAGIC Write the stream to an in-memory table
# MAGIC 0. Use appropriate `format`
# MAGIC 0. For this exercise, we want to append new records to the results table
# MAGIC 0. Gives the query a name
# MAGIC 0. Start the query
# MAGIC 0. Assign the query to `mappedTable`

# COMMAND ----------

# ANSWER
mappedQuery = (mappedDF 
 .writeStream                           # From the DataFrame get the DataStreamWriter
 .format("memory")                      # Specify the sink format as "memory"
 .outputMode("complete")                # Configure the output mode as "complete"
 .queryName("mappedTablePython")       # Name the query "mappedTablePython"
 .start()                               # Start the query
)

# COMMAND ----------

# TEST  - Run this cell to test your solution.
dbTest("SS-06-isActive", True, mappedQuery.isActive)
dbTest("SS-06-name", "mappedTablePython", mappedQuery.name)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Wait until stream is done initializing...

# COMMAND ----------

untilStreamIsReady("mappedTablePython")

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Step 11: Use SQL Syntax to Display a Few Rows</h2>
# MAGIC 
# MAGIC Do a basic SQL query to display all columns and, say, 10 rows.
# MAGIC 
# MAGIC ### Why are we doing this?

# COMMAND ----------

# TEST - Run this cell to test your solution.
try: tableExists = (spark.table("mappedTablePython") is not None)
except: tableExists = False
dbTest("SS-06-1", True, tableExists)  

firstRowCol = spark.sql("SELECT * FROM mappedTablePython limit 1").first()[0]
dbTest("SS-06-rowsExist", True, len(firstRowCol) > 0) 

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Step 12: Stop Streaming Jobs</h2>
# MAGIC 
# MAGIC Before we can conclude, we need to shut down all active streams.

# COMMAND ----------

# ANSWER
for streamingQuery in spark.streams.active:
  streamingQuery.stop()

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("SS-06-numActiveStreams", 0, len(spark.streams.active))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Congratulations: ALL DONE!!

# COMMAND ----------

# MAGIC %md
# MAGIC Don't forget to complete this short [feedback survey](https://www.surveymonkey.com/r/YCH8FYB).  Your input is extremely important and will shape future development.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>