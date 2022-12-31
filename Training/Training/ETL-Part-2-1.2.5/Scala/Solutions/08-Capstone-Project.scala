// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Capstone Project: Custom Transformations, Aggregating and Loading
// MAGIC 
// MAGIC The goal of this project is to populate aggregate tables using Twitter data.  In the process, you write custom User Defined Functions (UDFs), aggregate daily most trafficked domains, join new records to a lookup table, and load to a target database.
// MAGIC 
// MAGIC ## Audience
// MAGIC * Primary Audience: Data Engineers
// MAGIC * Additional Audiences: Data Scientists and Data Pipeline Engineers
// MAGIC 
// MAGIC ## Prerequisites
// MAGIC * Web browser: Chrome or Firefox
// MAGIC * Lesson: <a href="$./03-User-Defined-Functions">User Defined Functions</a> 
// MAGIC * Lesson: <a href="$./05-Joins-and-Lookup-Tables">Joins and Lookup Tables</a> 
// MAGIC * Lesson: <a href="$./06-Database-Writes">Database Writes</a> 
// MAGIC 
// MAGIC ## Instructions
// MAGIC 
// MAGIC The Capstone work for the previous course in this series (ETL: Part 1) defined a schema and created tables to populate a relational mode. In this capstone project you take the project further.
// MAGIC 
// MAGIC In this project you ETL JSON Twitter data to build aggregate tables that monitor trending websites and hashtags and filter malicious users using historical data.  Use these four exercises to achieve this goal:<br><br>
// MAGIC 
// MAGIC 1. **Parse tweeted URLs** using a custom UDF
// MAGIC 2. **Compute aggregate statistics** of most tweeted websites and hashtags by day
// MAGIC 3. **Join new data** to an existing dataset of malicious users
// MAGIC 4. **Load records** into a target database

// COMMAND ----------

// MAGIC %md
// MAGIC Run the following cell.

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 1: Parse Tweeted URLs
// MAGIC 
// MAGIC Some tweets in the dataset contain links to other websites.  Import and explore the dataset using the provided schema.  Then, parse the domain name from these URLs using a custom UDF.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1: Import and Explore
// MAGIC 
// MAGIC The following is the schema created as part of the capstone project for ETL Part 1.  
// MAGIC Run the following cell and then use this schema to import one file of the Twitter data.

// COMMAND ----------

import org.apache.spark.sql.types.{StructField, StructType, ArrayType, StringType, IntegerType, LongType}

lazy val fullTweetSchema = StructType(List(
  StructField("id", LongType, true),
  StructField("user", StructType(List(
    StructField("id", LongType, true),
    StructField("screen_name", StringType, true),
    StructField("location", StringType, true),
    StructField("friends_count", IntegerType, true),
    StructField("followers_count", IntegerType, true),
    StructField("description", StringType, true)
  )), true),
  StructField("entities", StructType(List(
    StructField("hashtags", ArrayType(
      StructType(List(
        StructField("text", StringType, true)
      ))
    ), true),
    StructField("urls", ArrayType(
      StructType(List(
        StructField("url", StringType, true),
        StructField("expanded_url", StringType, true),
        StructField("display_url", StringType, true)
      ))
    ), true) 
  )), true),
  StructField("lang", StringType, true),
  StructField("text", StringType, true),
  StructField("created_at", StringType, true)
))

// COMMAND ----------

// MAGIC %md
// MAGIC Import one file of the JSON data located at `/mnt/training/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4` using the schema.  Be sure to do the following:<br><br>
// MAGIC 
// MAGIC * Save the result to `tweetDF`
// MAGIC * Apply the schema `fullTweetSchema`
// MAGIC * Filter out null values from the `id` column

// COMMAND ----------

// ANSWER
val path = "/mnt/training/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4"
// val path = "/mnt/training/twitter/firehose/2018/*/*/*/*" // This imports of the data
val tweetDF = spark.read
  .schema(fullTweetSchema)
  .json(path)
  .filter($"id".isNotNull)

display(tweetDF)

// COMMAND ----------

// TEST - Run this cell to test your solution
dbTest("ET2-S-08-01-01", 1491, tweetDF.count)
dbTest("ET2-S-08-01-02", true, tweetDF.columns.contains("text") & tweetDF.columns.contains("id"))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step 2: Write a UDF to Parse URLs
// MAGIC 
// MAGIC The Scala regular expression library `Regex` allows you define a set of rules of a string you want to match. In this case, parse just the domain name  in a string for the URL of a link in a Tweet. Take a look at the following example that uses the `r` method provided for all strings:
// MAGIC 
// MAGIC ```
// MAGIC val pattern = """https?://(www\.)?([^/#\?]+)""".r
// MAGIC pattern.findFirstMatchIn("http://spark.apache.org/docs/latest/").map(_.group(2)).getOrElse("")
// MAGIC ```
// MAGIC 
// MAGIC This code returns `spark.apache.org`.  **Wrap this code into a function named `getDomain` that takes a parameter `URL` and returns the matched string.**
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> <a href="https://www.scala-lang.org/api/2.12.0/scala/util/matching/Regex.html" target="_blank">You can find more on the `Regex` library here.</a>

// COMMAND ----------

// ANSWER
def getDomain(URL: String) : String = {
  val pattern = """https?://(www\.)?([^/#\?]+)""".r
  pattern.findFirstMatchIn(URL).map(_.group(2)).getOrElse("")
}

getDomain("https://www.databricks.com/")

// COMMAND ----------

// TEST - Run this cell to test your solution
dbTest("ET2-S-08-02-01", "databricks.com",  getDomain("https://www.databricks.com/"))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 3: Test and Register the UDF
// MAGIC 
// MAGIC Now that the function works with a single URL, confirm that it works on different URL formats.

// COMMAND ----------

// MAGIC %md
// MAGIC Run the following cell to test your function further.

// COMMAND ----------

val urls = List(
  "https://www.databricks.com/",
  "https://databricks.com/",
  "https://databricks.com/training-overview/training-self-paced",
  "http://www.databricks.com/",
  "http://databricks.com/",
  "http://databricks.com/training-overview/training-self-paced",
  "http://www.apache.org/",
  "http://spark.apache.org/docs/latest/"
)

urls.map(getDomain).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC Register the UDF as `getDomainUDF`.

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions.udf

val getDomainUDF = sqlContext.udf.register("getDomainUDF", getDomain _)

// COMMAND ----------

// TEST - Run this cell to test your solution
dbTest("ET2-S-08-03-01", true, getDomainUDF.toString.contains("UserDefinedFunction"))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 4: Apply the UDF
// MAGIC 
// MAGIC Create a dataframe called `urlDF` that has three columns:<br><br>
// MAGIC 
// MAGIC 1. `URL`: The URL's from `tweetDF` (located in `entities.urls.expanded_url`) 
// MAGIC 2. `parsedURL`: The UDF applied to the column `URL`
// MAGIC 3. `created_at`
// MAGIC 
// MAGIC There can be zero, one, or many URLs in any tweet.  For this step, use the `explode` function, which takes an array like URLs and returns one row for each value in the array.
// MAGIC <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$@explode(e:org.apache.spark.sql.Column):org.apache.spark.sql.Column" target="_blank">See the documents here for details.</a>

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions.explode

val urlDF = (tweetDF
  .withColumn("URL", explode($"entities.urls.expanded_url"))
  .select("URL", "created_at") 
  .withColumn("parsedURL", getDomainUDF($"URL"))
)

display(urlDF)

// COMMAND ----------

// TEST - Run this cell to test your solution
val cols = urlDF.columns
val sample = urlDF.first

dbTest("ET2-S-08-04-01", true, cols.contains("URL") & cols.contains("parsedURL") & cols.contains("created_at"))
dbTest("ET2-S-08-04-02", "https://www.youtube.com/watch?v=b4iz9nZPzAA", sample.getAs[String]("URL"))
dbTest("ET2-S-08-04-03", "Mon Jan 08 18:47:59 +0000 2018", sample.getAs[String]("created_at"))
dbTest("ET2-S-08-04-04", "youtube.com", sample.getAs[String]("parsedURL"))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 2: Compute Aggregate Statistics
// MAGIC 
// MAGIC Calculate top trending 10 URLs by hour.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1: Parse the Timestamp
// MAGIC 
// MAGIC Create a DataFrame `urlWithTimestampDF` that includes the following columns:<br><br>
// MAGIC 
// MAGIC * `URL`
// MAGIC * `parsedURL`
// MAGIC * `timestamp`
// MAGIC * `hour`
// MAGIC 
// MAGIC Import `unix_timestamp` and `hour` from the `functions` module and `TimestampType` from the types `module`. To parse the `create_at` field, use `unix_timestamp` with the format `EEE MMM dd HH:mm:ss ZZZZZ yyyy`.

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions.{unix_timestamp, hour}
import org.apache.spark.sql.types.TimestampType

val timestampFormat = "EEE MMM dd HH:mm:ss ZZZZZ yyyy"

val urlWithTimestampDF = urlDF
  .withColumn("timestamp", unix_timestamp($"created_at", timestampFormat).cast(TimestampType).alias("createdAt"))
  .drop("created_at")
  .withColumn("hour", hour($"timestamp"))

display(urlWithTimestampDF)

// COMMAND ----------

// TEST - Run this cell to test your solution
val cols = urlWithTimestampDF.columns
val sample = urlWithTimestampDF.first

dbTest("ET2-S-08-05-01", true, cols.contains("URL") & cols.contains("parsedURL") & cols.contains("timestamp") & cols.contains("hour"))
dbTest("ET2-S-08-05-02", 18, sample.getAs[Int]("hour"))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 2: Calculate Trending URLs
// MAGIC 
// MAGIC Create a DataFrame `urlTrendsDF` that looks at the top 10 hourly counts of domain names and includes the following columns:<br><br>
// MAGIC 
// MAGIC * `hour`
// MAGIC * `parsedURL`
// MAGIC * `count`
// MAGIC 
// MAGIC The result should sort `hour` in ascending order and `count` in descending order.

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions.desc

val urlTrendsDF = (urlWithTimestampDF
  .groupBy("hour", "parsedURL")
  .count()
  .orderBy($"hour", desc("count"))
  .limit(10)
)

display(urlTrendsDF)

// COMMAND ----------

// TEST - Run this cell to test your solution
val cols = urlTrendsDF.columns
val sample = urlTrendsDF.first

dbTest("ET2-S-08-06-01", true, cols.contains("hour") & cols.contains("parsedURL") & cols.contains("count"))
dbTest("ET2-S-08-06-02", 18, sample.getAs[Int]("hour"))
dbTest("ET2-S-08-06-02", "twitter.com", sample.getAs[String]("parsedURL"))
dbTest("ET2-S-08-06-03", 159, sample.getAs[Long]("count"))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 3: Join New Data
// MAGIC 
// MAGIC Filter out bad users.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1: Import Table of Bad Actors
// MAGIC 
// MAGIC Create the DataFrame `badActorsDF`, a list of bad actors that sits in `/mnt/training/twitter/supplemental/badactors.parquet`.

// COMMAND ----------

// ANSWER
val badActorsDF = spark.read.parquet("/mnt/training/twitter/supplemental/badactors.parquet")

display(badActorsDF)

// COMMAND ----------

// TEST - Run this cell to test your solution
val cols = badActorsDF.columns
val sample = badActorsDF.first

dbTest("ET2-S-08-07-01", true, cols.contains("userID") & cols.contains("screenName"))
dbTest("ET2-S-08-07-02", 4875602384L, sample.getAs[Long]("userID"))
dbTest("ET2-S-08-07-02", "cris_silvag1", sample.getAs[String]("screenName"))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 2: Add a Column for Bad Actors
// MAGIC 
// MAGIC Add a new column to `tweetDF` called `maliciousAcct` with `true` if the user is in `badActorsDF`.  Save the results to `tweetWithMaliciousDF`.  Remember to do a left join of the malicious accounts on `tweetDF`.

// COMMAND ----------

// ANSWER
val tweetWithMaliciousDF = tweetDF
  .join(badActorsDF, tweetDF.col("user.id") === badActorsDF.col("userID"), "left")
  .withColumn("maliciousAcct", $"userID".isNotNull)
  .drop("screen_name", "userID")

display(tweetWithMaliciousDF)

// COMMAND ----------

// TEST - Run this cell to test your solution
val cols = tweetWithMaliciousDF.columns
val sample = tweetWithMaliciousDF.first

dbTest("ET2-S-08-08-01", true, cols.contains("maliciousAcct") & cols.contains("id"))
dbTest("ET2-S-08-08-02", 950438954272096257L, sample.getAs[Long]("id"))
dbTest("ET2-S-08-08-02", false, sample.getAs[Boolean]("maliciousAcct"))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 4: Load Records
// MAGIC 
// MAGIC Transform your two DataFrames to 4 partitions and save the results to the following endpoints:
// MAGIC 
// MAGIC | DataFrame              | Endpoint                            |
// MAGIC |:-----------------------|:------------------------------------|
// MAGIC | `urlTrendsDF`          | `userhome + /tmp/urlTrends.parquet`            |
// MAGIC | `tweetWithMaliciousDF` | `userhome + /tmp/tweetWithMaliciousDF.parquet` |

// COMMAND ----------

// ANSWER
urlTrendsDF.repartition(4).write.mode("overwrite").parquet(userhome + "/tmp/urlTrends.parquet")
tweetWithMaliciousDF.repartition(4).write.mode("overwrite").parquet(userhome + "/tmp/tweetWithMaliciousDF.parquet")

// COMMAND ----------

// TEST - Run this cell to test your solution
val urlTrendsDFTemp = spark.read.parquet(userhome + "/tmp/urlTrends.parquet")
val tweetWithMaliciousDFTemp = spark.read.parquet(userhome + "/tmp/tweetWithMaliciousDF.parquet")

dbTest("ET2-S-08-09-01", 4, urlTrendsDFTemp.rdd.getNumPartitions)
dbTest("ET2-S-08-09-02", 4, tweetWithMaliciousDFTemp.rdd.getNumPartitions)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## IMPORTANT Next Steps
// MAGIC * Please complete the <a href="https://www.surveymonkey.com/r/VYGM9TD" target="_blank">short feedback survey</a>.  Your input is extremely important and shapes future course development.
// MAGIC * Congratulations, you have completed ETL Part 2!

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>