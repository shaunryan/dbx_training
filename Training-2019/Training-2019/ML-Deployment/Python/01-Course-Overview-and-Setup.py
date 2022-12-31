# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Course Overview and Setup
# MAGIC ## Machine Learning Deployment
# MAGIC ### 3 Model Deployment Paradigms, Monitoring, and Alerting
# MAGIC 
# MAGIC This course teaches data scientists and data engineers best practices for deploying machine learning models into production.  First, it explores common production issues faced when deploying machine learning solutions.  Second, it implements various deployment options including batch, continuous with Spark Streaming, and on demand with RESTful and containerized services.  This includes integrations with databases, data streams, and hosted endpoints.  Finally, it covers monitoring machine learning models once they have been deployed into production.
# MAGIC 
# MAGIC By the end of this course, you will have built the infrastructure to deploy and monitor machine learning models.
# MAGIC 
# MAGIC ** Prerequisites:**<br><br>
# MAGIC 
# MAGIC - **Azure Portal permissions and the appropriate resources to build the infrastructure involved in the course.**
# MAGIC - Python (`pandas`, `sklearn`, `numpy`)
# MAGIC - Background in machine learning and data science
# MAGIC - Basic knowledge of Spark DataFrames (recommended)
# MAGIC 
# MAGIC ** The course consists of the following lessons:**<br><br>
# MAGIC 
# MAGIC 0. Course Overview and Setup
# MAGIC 0. Production Issues
# MAGIC 0. Batch Deployment
# MAGIC 0. Streaming Deployment
# MAGIC 0. Real Time Deployment
# MAGIC 0. Drift Monitoring
# MAGIC 0. Alerting
# MAGIC 0. Capstone Project

# COMMAND ----------

# MAGIC %md
# MAGIC # Getting Started on Databricks
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Introduce Machine Learning Deployment
# MAGIC * Motivate a systematic and agile approach to machine learning deployment
# MAGIC 
# MAGIC ## Audience
# MAGIC * Data Scientists 
# MAGIC * Data Engineers

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Agile Data Science
# MAGIC 
# MAGIC Deploying machine learning models into production comes with a wide array of challenges, distinct from those data scientists face when they're initially training models.  Teams often solve these challenges with custom, in-house solutions that are often brittle, monolithic, time consuming, and difficult to maintain.
# MAGIC 
# MAGIC A systematic approach to the deployment of machine learning models results in an agile solution that minimizes developer time and maximizes the business value derived from data science.  To achieve this, data scientists and data engineers need to navigate various deployment solutions as well as have a system in place for monitoring and alerting once a model is out in production.
# MAGIC 
# MAGIC The main deployment paradigms are as follows:<br><br>
# MAGIC 
# MAGIC 1. **Batch:** predictions are created and stored for later use, such as a database that can be queried in real time in a web application
# MAGIC 2. **Streaming:** data streams are transformed where the prediction is needed soon after it arrives in a data pipeline but not immediately
# MAGIC 3. **Real time:** normally implemented with a REST endpoint, a prediction is needed on the fly with low latency
# MAGIC 4. **Mobile/Embedded:** entails embedding machine learning solutions in mobile or IoT devices and is outside the scope of this course
# MAGIC 
# MAGIC Once a model is deployed in one of these paradigms, it needs to be monitored for performance with regards to the quality of predictions, latency, throughput, and other production considerations.  When performance starts to slip, this is an indication that the model needs to be retrained, more resources need to be allocated to serving the model, or any number of improvements are needed.  An alerting infrastructure needs to be in place to capture these performance issues.
# MAGIC 
# MAGIC <br>
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/ml-stock.jpg" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>