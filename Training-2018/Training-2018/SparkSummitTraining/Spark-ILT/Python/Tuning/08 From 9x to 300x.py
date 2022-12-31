# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #From 9x Faster to 300x Faster
# MAGIC 
# MAGIC **Dataset:**
# MAGIC * This is synthetic data generated specifically for these exercises
# MAGIC * Each year's data is roughly the same with some variation for market growth
# MAGIC * We are looking at retail purchases from the top N retailers

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This notebook is only available in Scala due to the use of APIs not available in Python.<br/>
# MAGIC However, the fundamental principles taught here remain the same for both languages.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %run "../Includes/Initialize Labs"

# COMMAND ----------

# MAGIC %run "../Includes/Utility Methods"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Ready, Set, Go!
# MAGIC 
# MAGIC * Some of the taks below will take a long time to run
# MAGIC * We can expidite things by running **ALL** the cells first
# MAGIC * While they are running, we can review the problem, solutions and the fix
# MAGIC 
# MAGIC <img style="float:left; margin-right:1em; box-shadow: 5px 5px 5px 0px rgba(0,0,0,0.25); border: 1px solid rgba(0,0,0,0.25);" src="https://files.training.databricks.com/images/notebook-cell-run-cmd.png"/>
# MAGIC <p><br/><br/><br/><img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** From the top of the notebook, select **Run All**<br/>
# MAGIC    or from a subsequent cell, select **Run All Below**.</p>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Introducing "tracker"
# MAGIC 
# MAGIC We are going to look at one more performance issue before moving on.
# MAGIC 
# MAGIC Namely how our "slow" data is actually slow for some other reasons.
# MAGIC 
# MAGIC To do that we are also going to look at one of the more advanced API components of Apache Spark
# MAGIC * Note the reference to **tracker** declared above
# MAGIC * This is a utility class for benchmarking code blocks, spark jobs and even the cache
# MAGIC * Open the notebook **Utility Methods** in a new tab
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> This is one of the many cases where the Python API lags behind the Scala version.

# COMMAND ----------

# MAGIC %md
# MAGIC An instance of this tool has already been declared for us.
# MAGIC 
# MAGIC We can see how it works here.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC 
# MAGIC val resultsSample = tracker.track(() => {
# MAGIC   spark.read.parquet("/mnt/training/global-sales/cities/all.parquet")
# MAGIC })
# MAGIC 
# MAGIC resultsSample.printTime()
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 2017 On Disk

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC var path2017 = "/mnt/training/global-sales/transactions/2017.parquet"
# MAGIC 
# MAGIC val slowCountFiles = dbutils.fs.ls(path2017).filter(_.size > 0).size
# MAGIC val slowTotalSize =  dbutils.fs.ls(path2017).filter(_.size > 0).map(_.size).sum
# MAGIC val slowAvgSize = slowTotalSize/slowCountFiles 
# MAGIC 
# MAGIC println(f"Count:      $slowCountFiles%,10d files")
# MAGIC println(f"Total Size: ${slowTotalSize/1000.0/1000.0}%,10.2f MB")
# MAGIC println(f"Avg Size:   ${slowAvgSize/1000.0}%,10.2f KB")
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%">
# MAGIC   <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/> Review 2017 On Disk</h2>
# MAGIC   
# MAGIC   <h3>&nbsp;</h3>
# MAGIC   <h3>What one detail stands out above all others?</h3>
# MAGIC   
# MAGIC   <h3>&nbsp;</h3>
# MAGIC   <h3>Based on everything we've seen so far, how would we go about fixing this?</h3>
# MAGIC   
# MAGIC   <h3>&nbsp;</h3>
# MAGIC   <h3>Assuming a specific solution, what parameters would you use?</h3>
# MAGIC   
# MAGIC   <h3>&nbsp;</h3>
# MAGIC   <h3>Before implementing our solution, let's grab numbers for both 2017-Fast &amp; 2017-Slow</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Loading the 2017-Fast &amp; 2017-Slow Benchmarks

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val logPath = "%s/test-results".format(userhome)
# MAGIC 
# MAGIC val results = loadYourTestMap(logPath)
# MAGIC // val results = loadYourTestMap("dbfs:/mnt/training-rw/global-sales/solutions/sample-results")
# MAGIC 
# MAGIC val duration17Slow = results("Duration 2017-Slow").toLong
# MAGIC val duration17Fast = results("Duration 2017-Fast").toLong
# MAGIC val times14vs17 = (duration17Slow - duration17Fast) / duration17Fast.toDouble
# MAGIC 
# MAGIC println("2017-Slow Duration: %,6d ms".format(duration17Slow))
# MAGIC println("2017-Fast Duration: %,6d ms".format(duration17Fast))
# MAGIC println("2017-Fast vs 2017-Slow: %.2f".format(times14vs17))
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%">
# MAGIC   <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/> Review 2017-Slow vs 2017-Fast</h2>
# MAGIC   
# MAGIC   <h3>&nbsp;</h3>
# MAGIC   <h3>These results are nothing new</h3>
# MAGIC   
# MAGIC   <h3>&nbsp;</h3>
# MAGIC   <h3>The key thing to focus on is the 20x to 30x performance increase</h3>
# MAGIC   
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Fixing 2017

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC var path2017 = "/mnt/training/global-sales/transactions/2017.parquet"
# MAGIC val trx2017DF = spark.read.parquet(path2017)
# MAGIC 
# MAGIC // In case you need to start over
# MAGIC // dbutils.fs.rm(fixedPath17, true)
# MAGIC 
# MAGIC val fixedPath17 = s"$userhome/fixed-2017.parquet"
# MAGIC trx2017DF.repartition(8).write.parquet(fixedPath17)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Benchmarking 2017-Fixed

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val fixedFast17 = spark.read.parquet(fixedPath17)
# MAGIC 
# MAGIC val resultsFixed = tracker.track(() => {
# MAGIC   fixedFast17.count()
# MAGIC })
# MAGIC 
# MAGIC val duration17Fixed = resultsFixed.duration
# MAGIC logYourTest(logPath, "Duration 2017-Fixed", duration17Fixed)
# MAGIC 
# MAGIC resultsFixed.printTime()
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val fixedCountFiles = dbutils.fs.ls(fixedPath17).filter(_.size > 0).size
# MAGIC val fixedTotalSize =  dbutils.fs.ls(fixedPath17).filter(_.size > 0).map(_.size).sum
# MAGIC val fixedAvgSize = fixedTotalSize/fixedCountFiles 
# MAGIC 
# MAGIC println(f"Fast  vs Slow:  ${(duration17Slow - duration17Fast) / duration17Fast}%,10dx faster")
# MAGIC println(f"Fixed vs Slow:  ${(duration17Slow - duration17Fixed) / duration17Fixed}%,10dx faster")
# MAGIC println("-"*80)
# MAGIC 
# MAGIC println(f"Slow File Count:   $slowCountFiles%,8d files")
# MAGIC println(f"Fixed File Count:  $fixedCountFiles%,8d files")
# MAGIC println("-"*80)
# MAGIC 
# MAGIC println(f"Slow Total Size:   ${slowTotalSize/1000.0/1000.0}%,8.2f MB")
# MAGIC println(f"Fixed Total Size:  ${fixedTotalSize/1000.0/1000.0}%,8.2f MB")
# MAGIC println("-"*80)
# MAGIC 
# MAGIC println(f"Slow Avg Size:   ${slowAvgSize/1000.0/1000.0}%,10.2f MB")
# MAGIC println(f"Fixed Avg Size:  ${fixedAvgSize/1000.0/1000.0}%,10.2f MB")
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>