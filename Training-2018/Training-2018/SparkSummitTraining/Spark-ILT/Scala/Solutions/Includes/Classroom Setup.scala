// Databricks notebook source
// MAGIC %run "./Setup Environment"

// COMMAND ----------

// MAGIC %run "./Dataset Mounts"

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC //*******************************************
// MAGIC // CHECK FOR REQUIRED VERIONS OF SPARK & DBR
// MAGIC //*******************************************
// MAGIC 
// MAGIC val dbrVersion = assertDbrVersion(4, 0)
// MAGIC val sparkVersion = assertSparkVersion(2, 3)
// MAGIC 
// MAGIC displayHTML(s"""
// MAGIC Checking versions...
// MAGIC   <li>Spark: $sparkVersion</li>
// MAGIC   <li>DBR: $dbrVersion</li>
// MAGIC   <li>Scala: $scalaVersion</li>
// MAGIC   <li>Python: ${spark.conf.get("com.databricks.training.python-version")}</li>
// MAGIC """)
// MAGIC 
// MAGIC //*******************************************
// MAGIC // ILT Specific functions
// MAGIC //*******************************************
// MAGIC 
// MAGIC // Utility method to count & print the number of records in each partition.
// MAGIC def printRecordsPerPartition(df:org.apache.spark.sql.Dataset[Row]):Unit = {
// MAGIC   import org.apache.spark.sql.functions._
// MAGIC   println("Per-Partition Counts:")
// MAGIC   val results = df.rdd                                   // Convert to an RDD
// MAGIC     .mapPartitions(it => Array(it.size).iterator, true)  // For each partition, count
// MAGIC     .collect()                                           // Return the counts to the driver
// MAGIC 
// MAGIC   results.foreach(x => println("* " + x))
// MAGIC }

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC dbrVersion = assertDbrVersion(4, 0)
// MAGIC sparkVersion = assertSparkVersion(2, 3)
// MAGIC 
// MAGIC #*******************************************
// MAGIC # ILT Specific functions
// MAGIC #*******************************************
// MAGIC 
// MAGIC # Utility method to count & print the number of records in each partition.
// MAGIC def printRecordsPerPartition(df):
// MAGIC   print("Per-Partition Counts:")
// MAGIC   def countInPartition(iterator): yield __builtin__.sum(1 for _ in iterator)
// MAGIC   results = (df.rdd                   # Convert to an RDD
// MAGIC     .mapPartitions(countInPartition)  # For each partition, count
// MAGIC     .collect()                        # Return the counts to the driver
// MAGIC   )
// MAGIC   # Print out the results.
// MAGIC   for result in results: print("* " + str(result))
// MAGIC 
// MAGIC None # suppress output

// COMMAND ----------

// MAGIC %scala
// MAGIC displayHTML("All done!")