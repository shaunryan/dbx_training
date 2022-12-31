# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Delta Batch Operations - Upsert
# MAGIC 
# MAGIC Databricks&reg; Delta allows you to read, write and query data in data lakes in an efficient manner.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Use Databricks Delta to UPSERT data into existing Databricks Delta tables
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers 
# MAGIC * Secondary Audience: Data Analysts and Data Scientists
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
# MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
# MAGIC * Databricks Runtime 4.2 or greater
# MAGIC * Completed courses Spark-SQL, DataFrames or ETL-Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>, or have similar knowledge
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC We will use online retail datasets from
# MAGIC * `/mnt/training/online_retail` in the demo part and
# MAGIC * `/mnt/training/structured-streaming/events/` in the exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup-04"

# COMMAND ----------

# MAGIC %md
# MAGIC Set up relevant paths.

# COMMAND ----------

deltaMiniDataPath = basePath + "/customer-data-mini"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## UPSERT 
# MAGIC 
# MAGIC Literally means "UPdate" and "inSERT". It means to atomically either insert a row, or, if the row already exists, UPDATE the row.
# MAGIC 
# MAGIC It is also called **MERGE INTO**, which is what the Databricks Delta operation is called.  
# MAGIC 
# MAGIC Alter the data by changing the values in one of the columns for a specific `CustomerID`.
# MAGIC 
# MAGIC Let's load the CSV file `/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv`.

# COMMAND ----------

miniDataInputPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv"
inputSchema = "InvoiceNo STRING, StockCode STRING, Description STRING, Quantity INT, InvoiceDate STRING, UnitPrice DOUBLE, CustomerID INT, Country STRING"

miniDataDF = (spark.read          
  .option("header", "true")
  .schema(inputSchema)
  .csv(miniDataInputPath)                            
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPSERT Using Non-Databricks Delta Pipeline
# MAGIC 
# MAGIC This feature is not supported in non-Delta pipelines.
# MAGIC 
# MAGIC To UPSERT means to "UPdate" and "inSERT". In other words, UPSERT is not an atomic operation. It is literally TWO operations. 
# MAGIC 
# MAGIC Running an UPDATE could invalidate data that is accessed by the subsequent INSERT operation.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## UPSERT Using Databricks Delta Pipeline
# MAGIC 
# MAGIC Using Databricks Delta, however, we can do UPSERTS.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> In this Lesson, we will explicitly create tables as SQL notation works better with UPSERT.

# COMMAND ----------

(miniDataDF
  .write
  .mode("overwrite")
  .format("delta")
  .save(deltaMiniDataPath) 
)

spark.sql("""
    CREATE TABLE IF NOT EXISTS {}.customer_data_delta_mini
    USING DELTA 
    LOCATION '{}' 
  """.format(databaseName, deltaMiniDataPath))

# COMMAND ----------

# MAGIC %md
# MAGIC List all rows with `CustomerID=20993`.

# COMMAND ----------

sqlCmd = "SELECT * FROM {}.customer_data_delta_mini WHERE CustomerID=20993".format(databaseName)
display(spark.sql(sqlCmd))

# COMMAND ----------

# MAGIC %md
# MAGIC Form a new DataFrame where `StockCode` is `99999` for `CustomerID=20993`.
# MAGIC 
# MAGIC Create a table `customer_data_delta_to_upsert` that contains this data.

# COMMAND ----------

from pyspark.sql.functions import lit, col
customerSpecificDF = (miniDataDF
  .filter("CustomerID=20993")
  .withColumn("StockCode", lit(99999))
 )

spark.sql("DROP TABLE IF EXISTS {}.customer_data_delta_to_upsert".format(databaseName))
customerSpecificDF.write.saveAsTable("{}.customer_data_delta_to_upsert".format(databaseName))

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert the new data into `customer_data_delta_mini`.
# MAGIC 
# MAGIC Upsert is done using the `MERGE INTO` syntax.

# COMMAND ----------

spark.sql("USE {}".format(databaseName))

sqlCmd = """
  MERGE INTO customer_data_delta_mini
  USING customer_data_delta_to_upsert
  ON customer_data_delta_mini.CustomerID = customer_data_delta_to_upsert.CustomerID
  WHEN MATCHED THEN
    UPDATE SET
      customer_data_delta_mini.StockCode = customer_data_delta_to_upsert.StockCode
  WHEN NOT MATCHED
    THEN INSERT (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
    VALUES (
      customer_data_delta_to_upsert.InvoiceNo,
      customer_data_delta_to_upsert.StockCode, 
      customer_data_delta_to_upsert.Description, 
      customer_data_delta_to_upsert.Quantity, 
      customer_data_delta_to_upsert.InvoiceDate, 
      customer_data_delta_to_upsert.UnitPrice, 
      customer_data_delta_to_upsert.CustomerID, 
      customer_data_delta_to_upsert.Country)"""
spark.sql(sqlCmd)

# COMMAND ----------

# MAGIC %md
# MAGIC Notice how this data is seamlessly incorporated into `customer_data_delta_mini`.

# COMMAND ----------

sqlCmd = "SELECT * FROM {}.customer_data_delta_mini WHERE CustomerID=20993".format(databaseName)
display(spark.sql(sqlCmd))

# COMMAND ----------

# MAGIC %md
# MAGIC # LAB

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 1
# MAGIC 
# MAGIC Write base data to `deltaIotPath`.
# MAGIC 
# MAGIC We do this for you, so just run the cell below.

# COMMAND ----------

from pyspark.sql.functions import expr, col, from_unixtime, to_date
jsonSchema = "action string, time long"
streamingEventPath = "/mnt/training/structured-streaming/events/"
deltaIotPath = basePath + "/iot-pipeline"

(spark.read 
  .schema(jsonSchema)
  .json(streamingEventPath) 
  .withColumn("date", to_date(from_unixtime(col("time").cast("Long"),"yyyy-MM-dd")))
  .withColumn("deviceId", expr("cast(rand(5) * 100 as int)"))
  .repartition(200)
  .write
  .mode("overwrite")
  .format("delta")
  .partitionBy("date")
  .save(deltaIotPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 2
# MAGIC 
# MAGIC Create a DataFrame out of the the data sitting in `deltaIotPath`.

# COMMAND ----------

# TODO
deltaIotPath = basePath + "/iot-pipeline"

newDataDF =  spark.sql("FILL_IN".format(deltaIotPath))

# COMMAND ----------

# TEST  - Run this cell to test your solution.
schema = str(newDataDF.schema)

dbTest("assert-1", True, "action,StringType" in schema)
dbTest("assert-2", True, "time,LongType" in schema)
dbTest("assert-3", True, "date,DateType" in schema)
dbTest("assert-4", True, "deviceId,IntegerType" in schema)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 3
# MAGIC 
# MAGIC Create another DataFrame `newDeviceIdDF`
# MAGIC * Pick up the 1st row you see that has `action` set to `Open`.
# MAGIC   - <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Use the `limit(1)` method.
# MAGIC * Change `action` to `Close`.
# MAGIC   - <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Use the `lit()` function.
# MAGIC * We will use the associated `deviceId` in the cells that follow.
# MAGIC * The DataFrame you construct should only have 1 row.

# COMMAND ----------

# TODO
from pyspark.sql.functions import col, lit

newDeviceIdDF = (newDataDF
 FILL_IN
)

# COMMAND ----------

# TEST - Run this cell to test your solution.
actionCount = newDeviceIdDF.filter(col("Action") == "Close").count()

dbTest("Delta-L4-actionCount", 1, actionCount)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4
# MAGIC 
# MAGIC Write to a new Databricks Delta table named `iot_data_delta_to_upsert` that contains just our data to be upserted.

# COMMAND ----------

# TODO
spark.sql("FILL_IN")
newDeviceIdDF.write.saveAsTable("FILL_IN")

# COMMAND ----------

# TEST - Run this cell to test your solution.
count = spark.table("{}.iot_data_delta_to_upsert".format(databaseName)).count()

dbTest("Delta-04-demoIotTableHasRow", True, count > 0)  
  
print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5
# MAGIC 
# MAGIC Create a Databricks Delta table named `demo_iot_data_delta` that contains just the data from `deltaIotPath`.

# COMMAND ----------

# TODO
sqlCmd = """FILL_IN""".format(databaseName, deltaIotPath)

spark.sql(sqlCmd)

# COMMAND ----------

# TEST - Run this cell to test your solution.
try:
  tableExists = (spark.table("{}.demo_iot_data_delta".format(databaseName)).count() > 0)
except:
  tableExists = False
  
dbTest("Delta-04-demoTableExists", True, tableExists)  

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6
# MAGIC 
# MAGIC Insert the data `iot_data_delta_to_upsert` into `demo_iot_data_delta`.
# MAGIC 
# MAGIC You can adapt the SQL syntax for the upsert from our demo example, above.

# COMMAND ----------

# TODO
spark.sql("USE {}".format(databaseName))

sqlCmd = """
  FILL_IN
  """

spark.sql(sqlCmd)

# COMMAND ----------

# TEST - Run this cell to test your solution.
devId = newDeviceIdDF.select("deviceId").first()[0]

sqlCmd1 = "SELECT count(*) as total FROM {}.demo_iot_data_delta WHERE deviceId = {} AND action = 'Open' ".format(databaseName, devId)
countOpen = spark.sql(sqlCmd1).first()[0]

sqlCmd2 = "SELECT count(*) as total FROM {}.demo_iot_data_delta WHERE deviceId = {} AND action = 'Close' ".format(databaseName, devId)
countClose = spark.sql(sqlCmd2).first()[0]

dbTest("Delta-L4-count", True, countOpen == 0 and countClose > 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7
# MAGIC 
# MAGIC Count the number of items in `demo_iot_data_delta` where 
# MAGIC * `deviceId` is obtained from this query `newDeviceIdDF.select("deviceId").first()[0]` .
# MAGIC * `action` is `Close`.

# COMMAND ----------

# TODO
sqlCmd = "FILL_IN".format(databaseName, deltaIotPath)
count = spark.sql(sqlCmd).first()(0)

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("Delta-L4-demoiot-count", True, count > 0)

print("Tests passed!")

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Cleanup-04"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC In this Lesson we:
# MAGIC * Learned that is not possible to do UPSERTS in the traditional pre-Databricks Delta lake.
# MAGIC   - UPSERT is essentially two operations in one step 
# MAGIC   - UPdate and inSERT
# MAGIC * `MERGE INTO` is the SQL expression we use to do UPSERTs.
# MAGIC * Used Databricks Delta to UPSERT data into existing Databricks Delta tables.
# MAGIC * Ended up creating tables explicitly because it is easier to work with SQL syntax.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="https://docs.databricks.com/delta/delta-batch.html#" target="_blank">Table Batch Read and Writes</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>