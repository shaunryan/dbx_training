# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab: Runnable Notebooks
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lab you:<br>
# MAGIC - Create a parameterized, runnable notebook that logs an MLflow run

# COMMAND ----------

# MAGIC %run "./../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Runnable Notebook
# MAGIC 
# MAGIC [Fill out the notebook in this directory 04-Lab-Runnable.]($./04-Lab-Runnable)  It should accomplish the following:<br><br>
# MAGIC 
# MAGIC 0. Read in 3 parameters:
# MAGIC   - `n_estimators` with a default value of 100
# MAGIC   - `learning_rate` with a default value of .1
# MAGIC   - `max_depth` with a default value of 1
# MAGIC 0. Train and log a gradient boosted tree model
# MAGIC 0. Report back the path the model was saved to

# COMMAND ----------

# MAGIC %md
# MAGIC Run the notebook with the following code.

# COMMAND ----------

dbutils.notebook.run("./04-Lab-Runnable", 60, 
  {"n_estimators": "100",
   "learning_rate": ".1",
   "max_depth": "1"})

# COMMAND ----------

# MAGIC %md
# MAGIC Check that these logged runs also show up properly on the MLflow UI. 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Next Lesson<br>
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See the solutions folder for an example solution to this lab.

# COMMAND ----------

# MAGIC %md
# MAGIC ### [Start the next lesson, Model Management.]($../05-Model-Management )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>