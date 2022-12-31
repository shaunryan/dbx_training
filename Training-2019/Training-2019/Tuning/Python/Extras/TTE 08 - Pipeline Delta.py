# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Pipeline with Databricks
# MAGIC 
# MAGIC ![stream](https://kpistoropen.blob.core.windows.net/collateral/delta/Delta.png)
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - See how Databricks Delta reduces complexity and errors in your data pipeline

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

dbutils.fs.rm(userhome + "/tuning-p/delta_iot_pipeline", True)

# COMMAND ----------

from pyspark.sql.functions import expr

rawData = (spark
          .read
          .json("/databricks-datasets/structured-streaming/events/")
          .drop("time")
          .withColumn("date", expr("cast(concat('2018-01-', cast(rand(5) * 30 as int) + 1) as date)"))
          .withColumn("deviceId", expr("cast(rand(5) * 100 as int)")))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 1: Write out raw data and create our table

# COMMAND ----------

rawData.write.format("delta").partitionBy("date").save(userhome + "/tuning-p/delta_iot_pipeline/")

# COMMAND ----------

display(dbutils.fs.ls(userhome + "/tuning-p/delta_iot_pipeline/"))

# COMMAND ----------

display(dbutils.fs.ls(userhome + "/tuning-p/delta_iot_pipeline/date=2018-01-01/"))

# COMMAND ----------

import re

userSub = re.sub('\W', '_', username)
tableName = userSub + "_demo_iot_data_delta_p"
tablePath = userhome + "/tuning-p/delta_iot_pipeline"

sql("DROP TABLE IF EXISTS {}".format(tableName))
sql("""
CREATE TABLE {}
USING DELTA
LOCATION '{}'
""".format(tableName, tablePath))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 2: Query the data

# COMMAND ----------

display(sql("SELECT count(*) FROM {}".format(tableName)))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Worked great, no repair table necessary, since Delta automatically handles the metadata

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 3: Adding new data

# COMMAND ----------

new_data = (spark
            .range(100000)
            .selectExpr("'Open' as action", "cast('2018-01-30' as date) date")
            .withColumn("deviceId", expr("cast(rand(5) * 500 as int)")))

# COMMAND ----------

display(new_data)

# COMMAND ----------

new_data.write.format("delta").partitionBy("date").mode("append").save(userhome + "/tuning-p/delta_iot_pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 4: Query should show new results

# COMMAND ----------

display(sql("SELECT count(*) FROM {}".format(tableName)))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Again, no update necessary.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 5: Updating previous data

# COMMAND ----------

(new_data
.write
.format("delta")
.mode("overwrite") 
.option("replaceWhere", "date = cast('2018-01-30' as date)") 
.save(userhome + "/tuning-p/delta_iot_pipeline"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 6: Query should reflect new data

# COMMAND ----------

display(sql("SELECT count(*) FROM {}".format(tableName)))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 7: Add historical data

# COMMAND ----------

old_batch_data = (spark
                  .range(100000)
                  .selectExpr("'Open' as action", "cast(concat('2018-01-', cast(rand(5) * 15 as int) + 1) as date) as date")
                  .withColumn("deviceId", expr("cast(rand(5) * 100 as int)")))

old_batch_data.write.format("delta").partitionBy("date").mode("append").save(userhome + "/tuning-p/delta_iot_pipeline")

# COMMAND ----------

display(sql("SELECT count(*) FROM {}".format(tableName)))


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>