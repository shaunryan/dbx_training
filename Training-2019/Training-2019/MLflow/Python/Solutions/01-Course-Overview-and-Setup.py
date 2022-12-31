# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Course Overview and Setup
# MAGIC ## MLflow
# MAGIC ### Managing the Machine Learning Lifecycle
# MAGIC 
# MAGIC In this course data scientists and data engineers learn the best practices for managing experiments, projects, and models using MLflow.
# MAGIC 
# MAGIC By the end of this course, you will have built a pipeline to log and deploy machine learning models using the environment they were trained with.
# MAGIC 
# MAGIC ** Prerequisites:**<br><br>
# MAGIC 
# MAGIC - Python (`pandas`, `sklearn`, `numpy`)
# MAGIC - Background in machine learning and data science
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> This course only works in Azure Databricks or Amazon Web Services(AWS).  It does not currently work in Databricks Community Edition.
# MAGIC 
# MAGIC ** The course consists of the following lessons:**<br><br>
# MAGIC 
# MAGIC 0. Course Overview and Setup
# MAGIC 0. Experiment Tracking
# MAGIC 0. Packaging ML Projects
# MAGIC 0. Multistep Workflows
# MAGIC 0. Model Management 
# MAGIC 0. Capstone Project

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>