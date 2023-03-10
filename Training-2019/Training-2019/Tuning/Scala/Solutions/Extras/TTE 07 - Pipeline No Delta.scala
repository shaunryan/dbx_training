// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Spark Pipeline Without Delta
// MAGIC 
// MAGIC ![stream](https://kpistoropen.blob.core.windows.net/collateral/delta/non-delta-new.png)
// MAGIC 
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
// MAGIC  - See how complex a Spark pipeline is without Databricks Delta!

// COMMAND ----------

// MAGIC %run "../Includes/Classroom-Setup"

// COMMAND ----------

dbutils.fs.rm(userhome + "/tuning-s/iot-pipeline/", true)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Historical and new data is often written in very small files and very small directories (such as eventhub capture):
// MAGIC + This data is also partitioned by arrival time not event time!
// MAGIC 
// MAGIC ![stream](https://docs.microsoft.com/en-us/azure/data-lake-store/media/data-lake-store-archive-eventhub-capture/data-lake-store-eventhub-data-sample.png)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Step 0: Read data

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/structured-streaming/events/

// COMMAND ----------

import org.apache.spark.sql.functions._

val rawData = spark
              .read 
              .json("/databricks-datasets/structured-streaming/events/") 
              .drop("time") 
              .withColumn("date", expr("cast(concat('2018-01-', cast(rand(5) * 30 as int) + 1) as date)")) 
              .withColumn("deviceId", expr("cast(rand(5) * 100 as int)"))

// add a couple of columns for demo purposes

// COMMAND ----------

display(rawData)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Step 1: Write out raw data and create staging table

// COMMAND ----------

rawData.write.mode("overwrite").format("parquet").partitionBy("date").save(userhome + "/tuning-s/iot-pipeline/")

// COMMAND ----------

val tableName = username.replaceAll("\\W", "_") + "_demo_iot_data_s"
val tablePath = userhome + "/tuning-s/iot-pipeline"

sql(f"DROP TABLE IF EXISTS $tableName")
sql(f"""
CREATE TABLE $tableName (action STRING, deviceId INTEGER, date DATE)
USING parquet
OPTIONS (path = '$tablePath')
PARTITIONED BY (date)
""")

// COMMAND ----------

display(dbutils.fs.ls(userhome + "/tuning-s/iot-pipeline/"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Step 2: Query the data

// COMMAND ----------

display(sql(f"SELECT count(*) FROM $tableName"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Wait, no results? That's strange. Let's repair the table then.

// COMMAND ----------

display(sql(f"MSCK REPAIR TABLE $tableName"))

// COMMAND ----------

display(sql("SELECT count(*) FROM %s".format(tableName)))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Step 3: Appending new data

// COMMAND ----------

val new_data = spark
                .range(100000) 
                .selectExpr("'Open' as action", "'2018-01-30' date") 
                .withColumn("deviceId", expr("cast(rand(5) * 500 as int)"))

// COMMAND ----------

// MAGIC %md
// MAGIC ##Note: This is dangerous to simply append to the production table.

// COMMAND ----------

new_data.write.format("parquet").partitionBy("date").mode("append").save(userhome + "/tuning-s/iot-pipeline/")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Step 4: Query should show new results

// COMMAND ----------

display(sql(f"SELECT count(*) FROM $tableName"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC That's strange, well, we can repair the table again right.

// COMMAND ----------

display(sql(f"MSCK REPAIR TABLE $tableName"))

// COMMAND ----------

display(sql(f"SELECT count(*) FROM $tableName"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Step 5: Upserts / Changes (on previously written data)

// COMMAND ----------

new_data
  .drop("date")
  .write
  .format("parquet")
  .mode("overwrite")
  .save(userhome + "/tuning-s/iot-pipeline/date=2018-01-30/")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Step 6: Query should reflect new data

// COMMAND ----------

try {
  
  sql("SELECT count(*) FROM %s".format(tableName)).count()
  
} catch {
  case e:Exception => print("Looks like that didn't work:\n\n%s".format(e))
}

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC That's strange, guess we need to refresh the metadata.

// COMMAND ----------

display(sql(f"REFRESH TABLE $tableName"))

// COMMAND ----------

display(sql(f"SELECT count(*) FROM $tableName"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Step 7: Add historical data

// COMMAND ----------

val old_batch_data = spark
  .range(100000) 
  .selectExpr("'Open' as action", "cast(concat('2018-01-', cast(rand(5) * 15 as int) + 1) as date) as date") 
  .withColumn("deviceId", expr("cast(rand(5) * 100 as int)"))

old_batch_data
  .write
  .format("parquet")
  .partitionBy("date")
  .mode("append")
  .save(userhome + "/tuning-s/iot-pipeline/")

// COMMAND ----------

display(sql(f"SELECT count(*) FROM $tableName"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Won't be up to date until we call refresh

// COMMAND ----------

sql(f"MSCK REPAIR TABLE $tableName")
sql(f"REFRESH TABLE $tableName")

// COMMAND ----------

display(sql(f"SELECT count(*) FROM $tableName"))


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>