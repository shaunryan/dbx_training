# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #Apache Spark™ Tuning and Best Practices

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Your Personal Cluster
# MAGIC 
# MAGIC If you cluster already exists, just verify the settings are correct - edit & restart if necissary.
# MAGIC 
# MAGIC If your cluster does not yet exist, create your cluster as outlined below:

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC **Standard Configuration:**
# MAGIC * **Cluster Name**: Use your first name or pick a nickname for yourself. Avoid initials.
# MAGIC * **Cluster Mode**: Select <b style="color:blue">Standard</b>
# MAGIC * **Databricks Runtime Version**: Select the latest version, **unless instructed otherwise**
# MAGIC * **Python Version**: Select Python <b style="color:blue">3</b>
# MAGIC * **Driver Type**: Select <b style="color:blue">Same as worker</b>
# MAGIC * **Worker Type**: Select <b style="color:blue">Standard_D3_v2</b>.
# MAGIC   * <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> The default value looks very similar!
# MAGIC * **Enable autoscaling**: <b style="color:blue">Disable</b>, or rather, uncheck
# MAGIC * **Workers**: Please select only <b style="color:blue">2</b> workers. Selecting more will prevent the labs from functioning peoperly
# MAGIC * **Auto Termination**: <b style="color:blue">120 minutes</b>
# MAGIC 
# MAGIC This should yield a <b style="color:blue">42 GB</b> cluster with <b style="color:blue">8 cores</b>.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Use an appropriate cluster name. For example...

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) While We Wait
# MAGIC 
# MAGIC The amount of time required to start our cluster depends on a number of different factors:
# MAGIC * Lag in the cloud provider
# MAGIC * Number of simultanious requests for VMs (aka the size of the class)
# MAGIC * Worldwide events affecting the internet
# MAGIC * The phase of the moon
# MAGIC * etc.
# MAGIC 
# MAGIC So while we wait, let's talk clusters: **[Cluster Configurations]($../Extras/Cluster Configurations)**    

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC The call above declares a number of variables for us.
# MAGIC 
# MAGIC For example:

# COMMAND ----------

print('User Name: {}'.format(username))
print('User Home: {}'.format(userhome))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) About Temp Files
# MAGIC 
# MAGIC We will be creating temp files from time to time.
# MAGIC 
# MAGIC Below is an example of how to use the **userhome** variable.

# COMMAND ----------

path = '{}/some-topic/whatever.parquet'.format(userhome)
print(path)

# And if we were to use it to read in a 
# file it would look something like this:

# spark.read.parquet(path)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> The use of a personal temp directory drastically reduces collisions on the file system.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Ready to go?
# MAGIC 
# MAGIC Included in the notebook **Initialize Labs** are utiltiy methods to improve the class experience. 

# COMMAND ----------

# MAGIC %run "../Includes/Initialize Labs"

# COMMAND ----------

# MAGIC %md
# MAGIC For example, we provide utility methods for making various assertions as we progress:

# COMMAND ----------

clearYourResults()

validateYourAnswer("#1-Your Name", expectedHash=195428262, answer=username)
validateYourAnswer("#2-Ready to go", expectedHash=646192812, answer=True)
validateYourAnswer("#3-Only 8 Cores", expectedHash=1276280174, answer=sc.defaultParallelism)

# COMMAND ----------

summarizeYourResults()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; min-height:600px; width:100%">
# MAGIC   <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"/> A Quick Review</h2>
# MAGIC 
# MAGIC   <h3>&nbsp;</h3>
# MAGIC   <h3>How do Cores relate to Parallelism?</h3>
# MAGIC 
# MAGIC   <h3>&nbsp;</h3>
# MAGIC   <h3>How do Partitions relate to Tasks?</h3>
# MAGIC 
# MAGIC   <h3>&nbsp;</h3>
# MAGIC   <h3>How do Tasks relate to Cores? Threads?</h3>
# MAGIC 
# MAGIC   <h3>&nbsp;</h3>
# MAGIC   <h3>What is pipelining?</h3>
# MAGIC   
# MAGIC   <h3>&nbsp;</h3>
# MAGIC   <h3>What type of transformations mark a stage boundry? Actions?</h3>
# MAGIC   
# MAGIC   <h3>&nbsp;</h3>
# MAGIC   <h3>What is the relationship between a jobs, stages and tasks?</h3>
# MAGIC 
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>