// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Databricks Delta Batch Operations - Append
// MAGIC 
// MAGIC Databricks&reg; Delta allows you to read, write and query data in data lakes in an efficient manner.
// MAGIC 
// MAGIC ## In this lesson you:
// MAGIC * Append new records to a Databricks Delta table
// MAGIC 
// MAGIC ## Audience
// MAGIC * Primary Audience: Data Engineers 
// MAGIC * Secondary Audience: Data Analysts and Data Scientists
// MAGIC 
// MAGIC ## Prerequisites
// MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
// MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
// MAGIC * Databricks Runtime 4.2 or greater
// MAGIC * Completed courses Spark-SQL, DataFrames or ETL-Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>, or have similar knowledge
// MAGIC 
// MAGIC ## Datasets Used
// MAGIC We will use online retail datasets from
// MAGIC * `/mnt/training/online_retail` in the demo part and
// MAGIC * `/mnt/training/structured-streaming/events/` in the exercises

// COMMAND ----------

// MAGIC %md
// MAGIC ### Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup-03"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Refresh Base Data Set

// COMMAND ----------

val inputPath = "/mnt/training/online_retail/data-001/data.csv"
val inputSchema = "InvoiceNo STRING, StockCode STRING, Description STRING, Quantity INT, InvoiceDate STRING, UnitPrice DOUBLE, CustomerID INT, Country STRING"
val parquetDataPath  = basePath + "/customer-data/"

spark.read 
  .option("header", "true")
  .schema(inputSchema)
  .csv(inputPath) 
  .write
  .mode("overwrite")
  .format("parquet")
  .partitionBy("Country")
  .save(parquetDataPath)

// COMMAND ----------

// MAGIC %md
// MAGIC Create table out of base data set

// COMMAND ----------

spark.sql(s"""
  CREATE TABLE IF NOT EXISTS %s.customer_data 
  USING parquet 
  OPTIONS (path = "%s")
""".format(databaseName, parquetDataPath))

spark.sql("MSCK REPAIR TABLE %s.customer_data".format(databaseName))

// COMMAND ----------

// MAGIC %md
// MAGIC The original count of records is:

// COMMAND ----------

val sqlCmd = "SELECT count(*) FROM %s.customer_data".format(databaseName)
val origCount = spark.sql(sqlCmd).first()(0).asInstanceOf[Long]

println(origCount)
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Read in Some New Data

// COMMAND ----------

val inputSchema = "InvoiceNo STRING, StockCode STRING, Description STRING, Quantity INT, InvoiceDate STRING, UnitPrice DOUBLE, CustomerID INT, Country STRING"
val miniDataInputPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv"

val newDataDF = spark
  .read
  .option("header", "true")
  .schema(inputSchema)
  .csv(miniDataInputPath)

// COMMAND ----------

// MAGIC %md
// MAGIC Do a simple count of number of new items to be added to production data.

// COMMAND ----------

newDataDF.count()

// COMMAND ----------

// MAGIC %md
// MAGIC ## APPEND Using Non-Databricks Delta pipeline
// MAGIC 
// MAGIC Append the new data to `parquetDataPath`.

// COMMAND ----------

newDataDF
  .write
  .format("parquet")
  .partitionBy("Country")
  .mode("append")
  .save(parquetDataPath)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Let's count the rows in `customer_data`.
// MAGIC 
// MAGIC We expect to see `36` additional rows, but we do not.
// MAGIC 
// MAGIC Why not?
// MAGIC 
// MAGIC You will get the same count of old vs new records because the metastore doesn't know about the addition of new records yet.

// COMMAND ----------

val sqlCmd = "SELECT count(*) FROM %s.customer_data".format(databaseName)
val newCount = spark.sql(sqlCmd).first()(0).asInstanceOf[Long]
println("The old count of records is %s".format(origCount))
println("The new count of records is %s".format(newCount))
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Schema-on-Read Problem Revisited
// MAGIC 
// MAGIC We've added new data the metastore doesn't know about.
// MAGIC 
// MAGIC * It knows there is a `Sweden` partition, 
// MAGIC   - but it doesn't know about the 19 new records for `Sweden` that have come in.
// MAGIC * It does not know about the new `Sierra-Leone` partition, 
// MAGIC  - nor the 17 new records for `Sierra-Leone` that have come in.
// MAGIC 
// MAGIC Here are the the original table partitions:

// COMMAND ----------

lazy val sqlCmd = "SHOW PARTITIONS %s.customer_data".format(databaseName)

lazy val originalSet = spark.sql(sqlCmd).collect.toSet

originalSet.foreach(println)
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC Here are the partitions the new data belong to:

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS %s.mini_customer_data".format(databaseName))
newDataDF.write.partitionBy("Country").saveAsTable("%s.mini_customer_data".format(databaseName))

val sqlCmd = "SHOW PARTITIONS %s.mini_customer_data ".format(databaseName)

val newSet = spark.sql(sqlCmd).collect.toSet

newSet.foreach(println)
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC In order to get correct counts of records, we need to make these new partitions and new data known to the metadata.
// MAGIC 
// MAGIC To do this, we apply `MSCK REPAIR TABLE`.

// COMMAND ----------

val sqlCmd = "MSCK REPAIR TABLE %s.customer_data".format(databaseName)
spark.sql(sqlCmd)

// COMMAND ----------

// MAGIC %md
// MAGIC Count the number of records:
// MAGIC * The count should be correct now.
// MAGIC * That is, 65499 + 36 = 65535

// COMMAND ----------

val sqlCmd = "SELECT count(*) FROM %s.customer_data".format(databaseName)
println(spark.sql(sqlCmd).first()(0))
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Refresh Base Data Set, Write to Databricks Delta

// COMMAND ----------

val deltaDataPath  = basePath + "/customer-data-delta/"

spark.read 
  .option("header", "true")
  .schema(inputSchema)
  .csv(inputPath) 
  .write
  .mode("overwrite")
  .format("delta")
  .partitionBy("Country")
  .save(deltaDataPath) 

// COMMAND ----------

// MAGIC %md
// MAGIC ## APPEND Using Databricks Delta Pipeline
// MAGIC 
// MAGIC Next, repeat the process by writing to Databricks Delta format. 
// MAGIC 
// MAGIC In the next cell, load the new data in Databricks Delta format and save to `../delta/customer-data-delta/`.

// COMMAND ----------

val miniDataInputPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv"

(newDataDF
  .write
  .format("delta")
  .partitionBy("Country")
  .mode("append")
  .save(deltaDataPath)
)

// COMMAND ----------

// MAGIC %md
// MAGIC Perform a simple `count` query to verify the number of records and notice it is correct and does not first require a table repair.
// MAGIC 
// MAGIC Should have 36 more entries from before.

// COMMAND ----------

val sqlCmd = "SELECT count(*) FROM delta.`%s` ".format(deltaDataPath)
println(spark.sql(sqlCmd).first()(0))
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ## More Options?
// MAGIC 
// MAGIC Additional Databricks Delta Reader and Writer options are included in the [Extra folder]($./Extra/Delta-RW-Options).

// COMMAND ----------

// MAGIC %md
// MAGIC # LAB

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 1
// MAGIC 
// MAGIC 0. Apply the schema provided under the variable `jsonSchema`
// MAGIC 0. Read the JSON data under `streamingEventPath` into a DataFrame
// MAGIC 0. Add a `date` column using `to_date(from_unixtime($"time","yyyy-MM-dd"))`
// MAGIC 0. Add a `deviceId` column consisting of random numbers from 0 to 99 using this expression `expr("cast(rand(5) * 100 as int)")`
// MAGIC 0. Use the `repartition` method to split the data into 200 partitions
// MAGIC 
// MAGIC Refer to  <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$#" target="_blank">Spark Scala function documentation</a>.

// COMMAND ----------

// TODO
import org.apache.spark.sql.functions.{expr, from_unixtime, to_date}
val jsonSchema = "action string, time long"
val streamingEventPath = "/mnt/training/structured-streaming/events/"

val rawDataDF = spark
 .read 
 FILL_IN
 .repartition(200)

// COMMAND ----------

// TEST - Run this cell to test your solution.
val schema = rawDataDF.schema.mkString(",")
dbTest("assert-1", true, schema.contains("action,StringType"))
dbTest("assert-2", true, schema.contains("time,LongType"))
dbTest("assert-3", true, schema.contains("date,DateType"))
dbTest("assert-4", true, schema.contains("deviceId,IntegerType"))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 2
// MAGIC 
// MAGIC Write out the raw data.
// MAGIC * Use `overwrite` mode
// MAGIC * Use format `delta`
// MAGIC * Partition by `date`
// MAGIC * Save to `deltaIotPath`

// COMMAND ----------

// TODO
val deltaIotPath = basePath + "/iot-pipeline/"

rawDataDF
   .write
   FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.
spark.sql(s"""
  CREATE TABLE IF NOT EXISTS %s.iot_data_delta
  USING DELTA
  LOCATION '%s'""".format(databaseName, deltaIotPath))

var tableNotEmpty = false

try {
  tableNotEmpty = spark.table("%s.iot_data_delta".format(databaseName)).count() > 0
} catch {
  case e: Exception => tableNotEmpty = false
}

dbTest("Delta-02-backfillTableExists", true, tableNotEmpty)  

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 3
// MAGIC 
// MAGIC Create a new DataFrame with columns `action`, `time`, `date` and `deviceId`. The columns contain the following data:
// MAGIC 
// MAGIC * `action` contains the value `Open`
// MAGIC * `time` contains the Unix time cast into a long integer `cast(1529091520 as bigint)`
// MAGIC * `date` contains `cast('2018-06-01' as date)`
// MAGIC * `deviceId` contains a random number from 0 to 499 given by `expr("cast(rand(5) * 500 as int)")`

// COMMAND ----------

// TODO
import org.apache.spark.sql.functions.expr

val newDF = spark.range(10000) 
 .repartition(200)
 .selectExpr("'Open' as action", FILL_IN)
 .FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.
val total = newDF.count()

dbTest("Delta-03-newDF-count", 10000, total)
println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Step 4
// MAGIC 
// MAGIC Append new data to `deltaIotPath`
// MAGIC 
// MAGIC * Use `append` mode
// MAGIC * Use format `delta`
// MAGIC * Partition by `date`
// MAGIC * Save to `deltaIotPath`

// COMMAND ----------

// TODO
newDF
 .write
 FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.
lazy val numFiles = spark.sql("SELECT count(*) as total FROM delta.`%s`".format(deltaIotPath)).first()(0).asInstanceOf[Long]

dbTest("Delta-03-numFiles", 110000 , numFiles)

println("Tests passed!")

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Cleanup-03"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Summary
// MAGIC 
// MAGIC In this Lesson we:
// MAGIC * Encountered the schema-on-read problem when appending new data in a traditional data lake pipeline.
// MAGIC * Learned how to append new data to existing Databricks Delta data (that mitigates the above problem).
// MAGIC * Showed how to look at the set of partitions in the data set.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Topics & Resources
// MAGIC 
// MAGIC * <a href="https://docs.databricks.com/delta/delta-batch.html#" target="_blank">Delta Table Batch Read and Writes</a>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>