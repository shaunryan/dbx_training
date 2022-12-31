// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Scheduling Jobs Programatically
// MAGIC 
// MAGIC Apache Spark&trade; and Databricks&reg; can be automated through the Jobs UI, REST API, or the command line
// MAGIC 
// MAGIC ## In this lesson you:
// MAGIC * Submit jobs using the Jobs UI and REST API
// MAGIC * Monitor jobs using the REST API
// MAGIC 
// MAGIC ## Audience
// MAGIC * Primary Audience: Data Engineers
// MAGIC * Additional Audiences: Data Scientists and Data Pipeline Engineers
// MAGIC 
// MAGIC ## Prerequisites
// MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
// MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
// MAGIC * Concept (optional): <a href="https://academy.databricks.com/collections/frontpage/products/etl-part-1-data-extraction" target="_blank">ETL Part 1 course from Databricks Academy</a>
// MAGIC * Concept (optional): <a href="https://academy.databricks.com/products/etl-part-2-transformations-and-loads-1-user-1-year" target="_blank">ETL Part 2 course from Databricks Academy</a>

// COMMAND ----------

// MAGIC %md
// MAGIC <iframe  
// MAGIC src="//fast.wistia.net/embed/iframe/1ie2iv3vou?videoFoam=true"
// MAGIC style="border:1px solid #1cb1c2;"
// MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
// MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
// MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
// MAGIC <div>
// MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/1ie2iv3vou?seo=false">
// MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Automating ETL Workloads
// MAGIC 
// MAGIC Since recurring production jobs are the goal of ETL workloads, Spark needs a way to integrate with other automation and scheduling tools.  We also need to be able to run Python files and Scala/Java jars.
// MAGIC 
// MAGIC Recall from <a href="https://academy.databricks.com/collections/frontpage/products/etl-part-1-data-extraction" target="_blank">ETL Part 1 course from Databricks Academy</a> how we can schedule jobs using the Databricks user interface.  In this lesson, we'll explore more robust solutions to schedule jobs.
// MAGIC 
// MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-3/jobs.png" style="height: 400px; margin: 20px"/></div>
// MAGIC 
// MAGIC There are a number of different automation and scheduling tools including the following:<br><br>
// MAGIC 
// MAGIC * Command line tools integrated with the UNIX scheduler Cron
// MAGIC * The workflow scheduler Apache Airflow
// MAGIC * Microsoft's Scheduler or Data Factory
// MAGIC 
// MAGIC The gateway into job scheduling is programmatic access to Databricks, which can be achieved either through the REST API or the Databricks Command Line Interface (CLI).

// COMMAND ----------

// MAGIC %md
// MAGIC ### Access Tokens
// MAGIC 
// MAGIC Access tokens provide programmatic access to the Databricks CLI and REST API.  This lesson uses the REST API but could also be completed <a href="https://docs.azuredatabricks.net/user-guide/dev-tools/databricks-cli.html" target="_blank">using the command line alternative.</a>
// MAGIC 
// MAGIC To get started, first generate an access token.

// COMMAND ----------

// MAGIC %md
// MAGIC Run the cell below to mount the data.

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC In order to generate a token:<br><br>
// MAGIC 
// MAGIC 1. Click on the person icon in the upper-right corner of the screen.
// MAGIC 2. Click **User Settings**
// MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-3/token-1.png" style="height: 400px; margin: 20px"/></div>
// MAGIC 3. Click on **Access Tokens**
// MAGIC 4. Click on **Generate New Token**
// MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-3/token-2-azure.png" style="height: 400px; margin: 20px"/></div>
// MAGIC 
// MAGIC 5. Name your token
// MAGIC 6. Designate a lifespan (a shorter lifespan is generally better to minimize risk exposure)
// MAGIC 7. Click **Generate**
// MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-3/token-3.png" style="height: 400px; margin: 20px"/></div>
// MAGIC 8. Copy your token.  You'll only be able to see it once.
// MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-3/token-4.png" style="height: 400px; margin: 20px"/></div>
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Be sure to keep this key secure.  This grants the holder full programmatic access to Databricks, including both resources and data that's available to your Databricks environment.

// COMMAND ----------

// MAGIC %md
// MAGIC Paste your token into the following cell along with the domain of your Databricks deployment (you can see this in the notebook's URL).  The deployment should look something like `https://westus2.azuredatabricks.net`

// COMMAND ----------

val token = "FILL IN"
val domain = "https://<REGION>.azuredatabricks.net" + "/api/2.0/"

// COMMAND ----------

// MAGIC %md
// MAGIC Test that the connection works by listing all files in the root directory of DBFS.

// COMMAND ----------

val conn = new java.net.URL(domain+"dbfs/list?path=/").openConnection

conn.setRequestProperty("Authorization", "Bearer "+token)
scala.io.Source.fromInputStream(conn.getInputStream).mkString

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The REST API can be used at the command line using a command like `curl -s -H "Authorization: Bearer token" https://domain.cloud.databricks.com/api/2.0/dbfs/list\?path\=/`
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The CLI can be used with the command `databricks fs ls dbfs:/` once it has been installed and configured with your access token

// COMMAND ----------

// MAGIC %md
// MAGIC ### Scheduling with the REST API and CLI
// MAGIC 
// MAGIC Jobs can either be scheduled for running on a consistent basis or they can be run every time the API call is made.  Since there are many parameters in scheduling jobs, it's often best to schedule a job through the user interface, parse the configuration settings, and then run later jobs using the API.
// MAGIC 
// MAGIC Run the following cell to get the sense of what a basic job accomplishes.

// COMMAND ----------

val path = dbutils.notebook.run("./Runnable/Runnable-4", 60, Map("username" -> username, "ranBy" -> "NOTEBOOK"))
display(spark.read.parquet(path))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC The notebook `Runnable-4` logs a timestamp and how the notebook is run.  This will log our jobs.
// MAGIC 
// MAGIC Schedule this job notebook as a job using parameters by first navigating to the jobs panel on the left-hand side of the screen and creating a new job.  Customize the job as follows:<br><br>
// MAGIC 
// MAGIC 1. Give the job a name
// MAGIC 2. Choose the notebook `Runnable-4` in the `Runnable` directory of this course
// MAGIC 3. Add parameters for `username`, which is your Databricks login email (this gives you a unique path to save your data), and set `ranBy` as `JOB`
// MAGIC 4. Choose a cluster of 2 workers and 1 driver (the default is too large for our needs).  **You can also choose to run a job against an already active cluster, reducing the time to spin up new resources.**
// MAGIC 5. Click **Run now** to execute the job.
// MAGIC 
// MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-3/runnable-4-execution.png" style="height: 400px; margin: 20px"/></div>
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Set recurring jobs in the same way by adding a schedule
// MAGIC <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Set email alerts in case of job failure

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC When the job completes, paste the `Run ID` that appears under completed runs below.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> On the jobs page, you can access the logs to determine the cause of any failures.

// COMMAND ----------

val runId = "FILL_IN"

val conn = new java.net.URL(domain+s"jobs/runs/get?run_id=$runId").openConnection

conn.setRequestProperty("Authorization", "Bearer "+token)
scala.io.Source.fromInputStream(conn.getInputStream).mkString

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Two JSON parsing libraries come pre-installed with a Databricks cluster: <a href="http://json4s.org/" target="_blank">JSON4S</a> and <a href="https://github.com/FasterXML/jackson/" target="_blank">Jackson</a>.  If these don't suit your needs, you can always <a href="https://docs.databricks.com/user-guide/libraries.html" target="_blank">install your own library</a>

// COMMAND ----------

// MAGIC %md
// MAGIC Now take a look at the table to see the update

// COMMAND ----------

display(spark.read.parquet(path))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC With this design pattern, you can have full, programmatic access to Databricks.  <a href="https://docs.databricks.com/api/latest/examples.html#jobs-api-examples" target="_blank">See the documentation</a> for examples on submitting jobs from Python files and JARs and other API examples.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Always run production jobs on a new cluster to minimize the chance of unexpected behavior.  Autoscaling clusters allows for elastically allocating more resources to a job as needed

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 1: Create and Submit a Job using the REST API
// MAGIC 
// MAGIC Now that a job has been submitted through the UI, we can easily capture and re-run that job.  Re-run the job using the REST API and different parameters.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1: Create the `POST` Request Payload
// MAGIC 
// MAGIC To create a new job, communicate the specifications about the job using a `POST` request.  First, define the following variables:<br><br>
// MAGIC 
// MAGIC * `name`: The name of your job
// MAGIC * `notebook_path`: The path to the notebook `Runnable-4`.  This will be the `noteboook_path` variable listed in the API call above.

// COMMAND ----------

// ANSWER
val name = "Lesson-04-Lab"
val notebook_path = "/Shared/ETL-Part-3/Scala/Runnable/Runnable-4"

val dataStr = s"""{
  "notebook_task": {
    "notebook_path": "$notebook_path", 
    "base_parameters": {"username": $username, "ranBy": "REST-API"}
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

println(dataStr)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 2: Create the Job
// MAGIC 
// MAGIC Use the base `domain` defined above to create a URL for the REST endpoint `jobs/create`.  Then, submit a `POST` request using `data_str` as the payload.

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

// MAGIC %md
// MAGIC ### Step 3: Run the Job
// MAGIC 
// MAGIC Run the job using the `job_id` from above.  You'll need to submit the post request to the `RunEndPoint` URL of `jobs/run-now`

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
// MAGIC ### Step 4: Confirm that the Job Ran
// MAGIC 
// MAGIC Confirm that the job ran by checking the parquet file.  It can take a few minutes for the job to run and update this file.

// COMMAND ----------

display(spark.read.parquet(path))

// COMMAND ----------

// TEST - Run this cell to test your solution
val APICounts = spark.read.parquet(path)
  .filter($"ranBy" === "REST-API")
  .count

dbTest("ET3-S-04-01-01", true, APICounts > 0)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Review
// MAGIC **Question:** What ways can you schedule jobs on Databricks?  
// MAGIC **Answer:** Jobs can be scheduled using the UI, REST API, or Databricks CLI.
// MAGIC 
// MAGIC **Question:** How can you gain programmatic access to Databricks?  
// MAGIC **Answer:** Generating a token will give programmatic access to most Databricks services.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Next Steps
// MAGIC 
// MAGIC Start the next lesson, [Job Failure]($./05-Job-Failure ).

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Topics & Resources
// MAGIC 
// MAGIC **Q:** Where can I get more information on the REST API?  
// MAGIC **A:** Check out the <a href="https://docs.azuredatabricks.net/api/index.html" target="_blank">Databricks documentation.</a>
// MAGIC 
// MAGIC **Q:** How can I set up the Databricks CLI?  
// MAGIC **A:** Check out the <a href="https://docs.azuredatabricks.net/user-guide/dev-tools/databricks-cli.html#set-up-the-cli" target="_blank">Databricks documentation for step-by-step instructions.</a>
// MAGIC 
// MAGIC **Q:** How can I do a `spark-submit` job using the API?  
// MAGIC **A:** Check out the <a href="https://docs.azuredatabricks.net/api/latest/examples.html#spark-submit-api-example" target="_blank">Databricks documentation for API examples.</a>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>