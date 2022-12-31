// Databricks notebook source

spark.sql("set spark.databricks.delta.preview.enabled=true") // Confirm Delta is enabled

// COMMAND ----------

// TODO

FILL_IN # create the "readPath" widget
FILL_IN # create the "writePath" widget
FILL_IN # create the "checkpointLocation" widget

// COMMAND ----------

// TODO

val readPath = FILL_IN            // Assign "readPath" to the value of the corresponding widget
val writePath = FILL_IN           // Assign "writePath" to the value of the corresponding widget
val checkpointLocation = FILL_IN  // Assign "checkpointLocation" to the value of the corresponding widget

// COMMAND ----------

try {
  // Make sure the readPath actually exists
  dbutils.fs.ls(readPath)
} catch {
  case e:Exception => dbutils.notebook.exit("Failed to find readPath")
}

// COMMAND ----------

// TODO 

Using a DataStreamReader, create a DataFrame with the previously
specified parameter: readPath

tweetsDF = FILL_IN

// COMMAND ----------

// TODO 
 
Using a DataStreamWriter, write the data to either 
a delta table or parquet file with the specified 
parameters: checkpointLocation, writePath & Trigger.Once()

tweetsDF.writeStream.FILL_IN 

// COMMAND ----------

val count = spark.read.format("delta").load(writePath).count()

// COMMAND ----------

// TODO

Exit the notebook returning the 
count from the previous cell

dbutils.FILL_IN
