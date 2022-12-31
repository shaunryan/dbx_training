# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Alerting
# MAGIC 
# MAGIC Alerting allows you to announce the progress of different applications, which becomes increasingly important in automated production systems.  In this lesson, you explore basic alerting strategies using email and REST integration with tools like Slack.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you will:<br>
# MAGIC  - Explore the alerting landscape
# MAGIC  - Create a basic REST alert integrated with Slack
# MAGIC  - Create a more complex REST alert for Spark jobs using `SparkListener`

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Alerting Landscape
# MAGIC 
# MAGIC There are a number of different alerting tools with various levels of sophistication.<br><br>
# MAGIC * PagerDuty 
# MAGIC  - has become one of the most popular tools for monitoring production outages
# MAGIC  - allows for the escalation of issues across a team with alerts including text messages and phone calls
# MAGIC * Slack
# MAGIC * Twilio   
# MAGIC * Email alerts
# MAGIC 
# MAGIC Most alerting frameworks allow for custom alerting done through REST integration.
# MAGIC 
# MAGIC One additional helpful tool for Spark workloads:
# MAGIC * the `SparkListener`
# MAGIC * can perform custom logic on various Cluster actions

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting Basic Alerts
# MAGIC 
# MAGIC Create a basic alert using a Slack endpoint.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Define a Slack webhook.  This has been done for you.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Define your own Slack webhook <a href="https://api.slack.com/incoming-webhooks#getting-started" target="_blank">Using these 4 steps.</a><br>
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This same approach applies to PagerDuty as well.

# COMMAND ----------

webhookMLProductionAPIDemo = "" # FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC Send a test message and check Slack.

# COMMAND ----------

def postToSlack(webhook, content):
  import requests
  from requests.exceptions import MissingSchema
  from string import Template
  t = Template('{"text": "${content}"}')
  
  try:
    response = requests.post(webhook, data=t.substitute(content=content), headers={'Content-Type': 'application/json'})
    return response
  except MissingSchema:
    print("Please define an appropriate API endpoint to hit by defining the variable `webhookMLProductionAPIDemo`")

postToSlack(webhookMLProductionAPIDemo, "This is my post from Python")

# COMMAND ----------

# MAGIC %md
# MAGIC Do the same thing using Scala.  This involves a bit more boilerplate and a different library.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val webhookMLProductionAPIDemo = "" // FILL_IN

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def postToSlack(webhook:String, content:String):Unit = {
# MAGIC   import org.apache.http.entity._
# MAGIC   import org.apache.http.impl.client.{HttpClients}
# MAGIC   import org.apache.http.client.methods.HttpPost
# MAGIC 
# MAGIC   val client = HttpClients.createDefault()
# MAGIC   val httpPost = new HttpPost(webhook)
# MAGIC   
# MAGIC   val payload = s"""{"text": "${content}"}"""
# MAGIC 
# MAGIC   val entity = new StringEntity(payload)
# MAGIC   httpPost.setEntity(entity)
# MAGIC   httpPost.setHeader("Accept", "application/json")
# MAGIC   httpPost.setHeader("Content-type", "application/json")
# MAGIC   
# MAGIC   try {
# MAGIC     val response = client.execute(httpPost)
# MAGIC     client.close()
# MAGIC   } catch {
# MAGIC     case cPE: org.apache.http.client.ClientProtocolException => println("Please define an appropriate API endpoint to hit by defining the variable webhookMLProductionAPIDemo")
# MAGIC   } 
# MAGIC }
# MAGIC 
# MAGIC postToSlack(webhookMLProductionAPIDemo, "This is my post from Scala")

# COMMAND ----------

# MAGIC %md
# MAGIC Now you can easily integrate custom logic back to Slack.

# COMMAND ----------

mse = .45

postToSlack(webhookMLProductionAPIDemo, "The newly trained model MSE is now {}".format(mse))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Using a `SparkListener`
# MAGIC 
# MAGIC A custom `SparkListener` allows for custom actions taken on cluster activity.  **This API is only available in Scala.**  Take a look at the following code.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See the <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.scheduler.SparkListener" target="_blank">`SparkListener` docs</a>.
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Be sure to update the webhook variable in the following cell.

# COMMAND ----------

# MAGIC %scala
# MAGIC // Package in a notebook helps to ensure a proper singleton
# MAGIC package com.databricks.academy
# MAGIC 
# MAGIC object SlackNotifyingListener extends org.apache.spark.scheduler.SparkListener {
# MAGIC   import org.apache.spark.scheduler._
# MAGIC 
# MAGIC   val webhook = "" // FILL_IN
# MAGIC   
# MAGIC   def postToSlack(content:String):Unit = {
# MAGIC     import org.apache.http.entity._
# MAGIC     import org.apache.http.impl.client.{HttpClients}
# MAGIC     import org.apache.http.client.methods.HttpPost
# MAGIC 
# MAGIC     val client = HttpClients.createDefault()
# MAGIC     val httpPost = new HttpPost(webhook)
# MAGIC 
# MAGIC     val payload = s"""{"text": "${content}"}"""
# MAGIC 
# MAGIC     val entity = new StringEntity(payload)
# MAGIC     httpPost.setEntity(entity)
# MAGIC     httpPost.setHeader("Accept", "application/json")
# MAGIC     httpPost.setHeader("Content-type", "application/json")
# MAGIC 
# MAGIC     try {
# MAGIC       val response = client.execute(httpPost)
# MAGIC       client.close()
# MAGIC     } catch {
# MAGIC       case cPE: org.apache.http.client.ClientProtocolException => println("Please define an appropriate API endpoint to hit by defining the variable webhookMLProductionAPIDemo")
# MAGIC     } 
# MAGIC   }
# MAGIC   
# MAGIC   override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
# MAGIC     postToSlack("Called when the application ends")
# MAGIC   }
# MAGIC 
# MAGIC   override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
# MAGIC     postToSlack("Called when the application starts")
# MAGIC   }
# MAGIC 
# MAGIC   override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
# MAGIC     postToSlack("Called when a new block manager has joined")
# MAGIC   }
# MAGIC 
# MAGIC   override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
# MAGIC     postToSlack("Called when an existing block manager has been removed")
# MAGIC   }
# MAGIC 
# MAGIC   override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
# MAGIC     postToSlack("Called when the driver receives a block update info.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
# MAGIC     postToSlack("Called when environment properties have been updated")
# MAGIC   }
# MAGIC 
# MAGIC   override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
# MAGIC     postToSlack("Called when the driver registers a new executor.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = {
# MAGIC     postToSlack("Called when the driver blacklists an executor for a Spark application.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onExecutorBlacklistedForStage(executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit = {
# MAGIC     postToSlack("Called when the driver blacklists an executor for a stage.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
# MAGIC     // This one is a bit on the noisy side so I'm pre-emptively killing it
# MAGIC     // postToSlack("Called when the driver receives task metrics from an executor in a heartbeat.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
# MAGIC     postToSlack("Called when the driver removes an executor.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = {
# MAGIC     postToSlack("Called when the driver re-enables a previously blacklisted executor.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
# MAGIC     postToSlack("Called when a job ends")
# MAGIC   }
# MAGIC 
# MAGIC   override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
# MAGIC     postToSlack("Called when a job starts")
# MAGIC   }
# MAGIC 
# MAGIC   override def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = {
# MAGIC     postToSlack("Called when the driver blacklists a node for a Spark application.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onNodeBlacklistedForStage(nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit = {
# MAGIC     postToSlack("Called when the driver blacklists a node for a stage.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = {
# MAGIC     postToSlack("Called when the driver re-enables a previously blacklisted node.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onOtherEvent(event: SparkListenerEvent): Unit = {
# MAGIC     postToSlack("Called when other events like SQL-specific events are posted.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onSpeculativeTaskSubmitted(speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit = {
# MAGIC     postToSlack("Called when a speculative task is submitted")
# MAGIC   }
# MAGIC 
# MAGIC   override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
# MAGIC     postToSlack("Called when a stage completes successfully or fails, with information on the completed stage.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
# MAGIC     postToSlack("Called when a stage is submitted")
# MAGIC   }
# MAGIC 
# MAGIC   override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
# MAGIC     postToSlack("Called when a task ends")
# MAGIC   }
# MAGIC 
# MAGIC   override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
# MAGIC     postToSlack("Called when a task begins remotely fetching its result (will not be called for tasks that do not need to fetch the result remotely).")
# MAGIC   }
# MAGIC 
# MAGIC   override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
# MAGIC     postToSlack("Called when a task starts")
# MAGIC   }
# MAGIC 
# MAGIC   override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {
# MAGIC     postToSlack("Called when an RDD is manually unpersisted by the application")
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC Register this Singleton as a `SparkListener`

# COMMAND ----------

# MAGIC %scala
# MAGIC sc.addSparkListener(com.databricks.academy.SlackNotifyingListener)

# COMMAND ----------

# MAGIC %md
# MAGIC Now run a basic DataFrame operation and observe the results in Slack.

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.read
# MAGIC   .option("header", true)
# MAGIC   .option("inferSchema", true)
# MAGIC   .parquet("/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.parquet")
# MAGIC   .count

# COMMAND ----------

# MAGIC %md
# MAGIC This will also work in Python.

# COMMAND ----------

(spark.read
  .option("header", True)
  .option("inferSchema", True)
  .parquet("/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.parquet")
  .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC When this is done, remove the listener.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** What are the most common alerting tools?  
# MAGIC **Answer:** PagerDuty tends to be the tool most used in production environments.  SMTP servers emailing alerts are also popular, as is Twilio for text message alerts.  Slack webhooks and bots can easily be written as well.
# MAGIC 
# MAGIC **Question:** How can I write custom logic to monitor Spark?  
# MAGIC **Answer:** The `SparkListener` API is only exposed in Scala.  This allows you to write custom logic based on your cluster activity.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Capstone Project<br>
# MAGIC 
# MAGIC ### [Start the Capstone Project]($./08-Capstone-Project )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find the alerting tools mentioned in this lesson?  
# MAGIC **A:** Check out <a href="https://www.twilio.com" target="_blank">Twilio</a> and <a href="https://www.pagerduty.com" target="_blank">PagerDuty</a>.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>