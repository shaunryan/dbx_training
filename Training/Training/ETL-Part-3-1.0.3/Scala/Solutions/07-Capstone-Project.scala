// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Capstone Project: Streaming and Failure Recovery
// MAGIC 
// MAGIC The goal of this project is to refactor a batch ETL job to a streaming job.  In the process, run the workload as a job and monitor it.
// MAGIC 
// MAGIC ## Audience
// MAGIC * Primary Audience: Data Engineers
// MAGIC * Additional Audiences: Data Scientists and Data Pipeline Engineers
// MAGIC 
// MAGIC ## Prerequisites
// MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
// MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
// MAGIC * Lesson: <a href="$./02-Streaming-ETL">Streaming ETL</a> 
// MAGIC * Lesson: <a href="$./03-Runnable-Notebooks">Runnable Notebooks</a> 
// MAGIC * Lesson: <a href="$./05-Job-Failure">Job Failure</a> 
// MAGIC 
// MAGIC ## Instructions
// MAGIC 
// MAGIC The Capstone work for for the previous courses in this series involved ETL workloads performed in batch on data at rest.
// MAGIC 
// MAGIC In this project, you ETL a stream of JSON Twitter data, execute it as a job, and develop a recovery strategy for failed jobs.

// COMMAND ----------

// MAGIC %md
// MAGIC Run the following cell to setup the environment.

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 1: Streaming ETL
// MAGIC 
// MAGIC Create a streaming job that monitors a directory for new files using trigger once semantics.  Write the result to a Databricks Delta table.  Keep this logic in a separate notebook.
// MAGIC 
// MAGIC [Use the notebook `Runnable-5-Stream` in the `Runnable` directory.]($./Runnable/Runnable-5-Stream )

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step 1: Explore the Workflow
// MAGIC 
// MAGIC The following cells accomplish the following:<br><br>
// MAGIC 
// MAGIC 1. Define a unique directory, `basePath = "{}/etl3s/capstone/".format(userhome)`, to store unparsed and parsed data as well as metadata.
// MAGIC 2. Clear the directory in case there is data already there.
// MAGIC 3. Create the function `addFile`, which copies Twitter JSON files into a directory for you to stream from.  You'll run this function multiple times to create a stream of files.
// MAGIC 
// MAGIC Run the following cell to define the objects.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> If you need to clear the `basePath` directory, use `dbutils.fs.rm(basePath, true)` to recursively remove all files.  **Note that this is a permanent action.**

// COMMAND ----------

val basePath = "%s/etl3s/capstone/".format(userhome)

val readPath = basePath + "files/"
val writePath = basePath + "processed/"
val checkpointLocation = basePath + "checkpoints.checkpoint"
var sampleFileCounter = 0

dbutils.fs.rm(basePath, true) // Confirm the path is empty

// adds a new sample Twitter file to the directory in path
def addFile(path: String, sampleFileCounter: Int): Int = {
  val tmpCounter = (sampleFileCounter % 5).toString // only 5 sample files, cycle through them
  val readPath = s"/mnt/training/twitter/sample/tweetSample$tmpCounter.json"
  val writePath = s"${path}tweetSample$sampleFileCounter.json"
  
  dbutils.fs.cp(readPath, writePath)
  println("Sample tweet file copied to " + writePath)
  
  sampleFileCounter + 1
}

// COMMAND ----------

// MAGIC %md
// MAGIC Run `addFile` to copy your first sample Twitter JSON file into your `username + "/academy/"` directory

// COMMAND ----------

sampleFileCounter = addFile(readPath, sampleFileCounter)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC Run the following cell to load the copied file into a DataFrame with a schema, and then display the DataFrame.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The Twitter data pulls unfiltered Tweets, some of which may contain adult content.  While it was sanitized, some adult content could have made it into the dataset.

// COMMAND ----------

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

val schema = StructType(List(
  StructField("tweet_id", LongType, true),
  StructField("text", StringType, true),
  StructField("screen_name", StringType, true),
  StructField("user_id", LongType, true),
  StructField("lang", StringType, true)
))

val sampleDF = (spark.read
  .schema(schema)
  .json(readPath+"tweetSample0.json")
)

display(sampleDF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step 2: Create a Streaming Notebook
// MAGIC 
// MAGIC Open [the notebook `Runnable-5-Stream` in the `Runnable` directory]($./Runnable/Runnable-5-Stream ) in a new tab.
// MAGIC 
// MAGIC The notebook should accomplish the following:<br><br>
// MAGIC 
// MAGIC 1. Take three parameters for `readPath`, `writePath`, and `checkpointLocation`
// MAGIC 2. Read a stream from `readPath`
// MAGIC   - Use `checkpointLocation`
// MAGIC 3. Write the stream to `writePath`
// MAGIC   - Use `checkpointLocation`
// MAGIC   - Use `.trigger(Trigger.Once())`
// MAGIC   - Use either the Delta or Parquet format
// MAGIC 4. Exit the notebook with the count of records in `writePath`
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Start by making the notebook run as expected.  Use dummy variables for `readPath`, `writePath`, and `checkpointLocation` (defined for you) that will later be replaced with parameters.  Once the notebook operates as expected, run it as a notebook with the following cell.

// COMMAND ----------

// ANSWER
val params = Map("readPath" -> readPath, 
  "writePath" -> writePath, 
  "checkpointLocation" -> checkpointLocation)

dbutils.notebook.run("./Runnable/Runnable-5-Stream", 60, params)

// COMMAND ----------

// TEST - Run this cell to test your solution
dbTest("ET3-S-07-01-01", true, spark.read.format("delta").load(writePath).count() >= 1964)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 3: Add Files and Rerun the Notebook
// MAGIC 
// MAGIC Add a new file and observe the change.

// COMMAND ----------

sampleFileCounter = addFile(readPath, sampleFileCounter)

// COMMAND ----------

dbutils.notebook.run("./Runnable/Runnable-5-Stream", 60, params)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 2: Trigger Jobs
// MAGIC 
// MAGIC Create and trigger a job for the notebook you created in Exercise 1.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1: Add a New File and Create a Job
// MAGIC 
// MAGIC Add a new file so the job has data to operate on.  Create a job to run it.

// COMMAND ----------

// MAGIC %md
// MAGIC Add a new file.

// COMMAND ----------

sampleFileCounter = addFile(readPath, sampleFileCounter)

// COMMAND ----------

// MAGIC %md
// MAGIC Define the token and domain for the REST API calls.

// COMMAND ----------

// ANSWER
val token = "FILL_IN"
val domain = "https://<REGION>.azuredatabricks.net" + "/api/2.0/"

// COMMAND ----------

// MAGIC %md
// MAGIC Define the POST request payload for your job, including the job name, notebook path, and parameters.

// COMMAND ----------

// ANSWER
val name = "ETL-3-Stream-Job"
val notebook_path = "/Shared/ETL-Part-3/Scala/Runnable/Runnable-5-Stream" // Your path could be different, likely including your username

val dataStr = s"""{
  "notebook_task": {
    "notebook_path": "$notebook_path", 
    "base_parameters": {"readPath": "$readPath", "writePath": "$writePath", "checkpointLocation": "$checkpointLocation"}
  }, 
  "new_cluster": {
    "node_type_id": "Standard_DS3_v2", 
    "num_workers": 2, 
    "spark_version": "4.2.x-scala2.11",
    "spark_conf": {"spark.databricks.delta.preview.enabled": "true"}
  }, 
  "name": "$name"
  }
"""

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 2: Create the Job
// MAGIC 
// MAGIC Create the job with a POST request to the `jobs/create` endpoint.  The result, when successful, is the job's id.  Save this to `job_id`.

// COMMAND ----------

// ANSWER
val createEndPoint = domain + "jobs/create"
val createConn = new java.net.URL(createEndPoint).openConnection.asInstanceOf[java.net.HttpURLConnection]

createConn.setRequestProperty("Authorization", "Bearer "+token)
createConn.setDoOutput(true)
createConn.setRequestMethod("POST")

val out = new java.io.OutputStreamWriter(createConn.getOutputStream)
out.write(dataStr)
out.flush
println(createConn.getResponseCode)

val responseJSON = scala.io.Source.fromInputStream(createConn.getInputStream).mkString
out.close

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC Run the job.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Note that, depending on how you wrote your code in `Runnable-5-Stream`, this could fail if you don't have a new file to parse.  You can add another file with `sampleFileCounter = addFile(readPath, sampleFileCounter)`

// COMMAND ----------

// ANSWER
val RunEndPoint = domain + "jobs/run-now"

val createConn2 = new java.net.URL(RunEndPoint).openConnection.asInstanceOf[java.net.HttpURLConnection]

createConn2.setRequestProperty("Authorization", "Bearer "+token)
createConn2.setDoOutput(true)
createConn2.setRequestMethod("POST")

val out = new java.io.OutputStreamWriter(createConn2.getOutputStream)
out.write(responseJSON)
out.flush
println(createConn2.getResponseCode)

val responseJSON2 = scala.io.Source.fromInputStream(createConn2.getInputStream).mkString
out.close

// COMMAND ----------

// MAGIC %md
// MAGIC Confirm that the job ran.

// COMMAND ----------

spark.read.parquet(writePath).count()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 3: Clean up the Files
// MAGIC 
// MAGIC Recursively delete the files you created (this is a permanent operation).

// COMMAND ----------

dbutils.fs.rm(username + "/academy/", true)

// COMMAND ----------

// MAGIC %md
// MAGIC ## IMPORTANT Next Steps
// MAGIC * Please complete the <a href="https://www.surveymonkey.com/r/NGYXJR6" target="_blank">short feedback survey</a>.  Your input is extremely important and shapes future course development.
// MAGIC * Congratulations, you have completed ETL Part 3!

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>