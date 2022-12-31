# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Agenda
# MAGIC ## Apache Spark for Machine Learning and Data Science
# MAGIC 
# MAGIC In this course data analysts and data scientists practice the full data science workflow by exploring data, building features, training models, and tuning and selecting the best model.  By the end of this course, you will have built end-to-end machine learning models ready to be used into production.
# MAGIC 
# MAGIC This course assumes basic data science familiarity with Sklearn and Pandas (or equivalent).
# MAGIC 
# MAGIC **Cluster Requirements (pre-configured):**
# MAGIC * 5.5 LTS ML 
# MAGIC * `mlflow==1.2.0` (PyPI)
# MAGIC * `Azure:mmlspark:0.15` (Maven)
# MAGIC * `koalas==0.15.0` (PyPI)
# MAGIC * `ml.combust.mleap:mleap-spark_2.11:0.13.0` (Maven)
# MAGIC * `spark-sklearn==0.3.0` (PyPI)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 1 AM
# MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 25m  | **Introductions & Setup**                               | *Registration, Courseware & Q&As* |
# MAGIC | 30m    | **[Spark Overview]($./ML 00 - Spark Overview)**    | Background on Apache Spark,  Spark Architecture |
# MAGIC | 35m  | **[Data Cleansing]($./ML 01 - Data Cleansing)** | How to deal with null values, outliers, data imputation | 
# MAGIC | 30m  | **Break (10:30 - 11:00)**                                               ||
# MAGIC | 30m  | **[Data Exploration Lab]($./Labs/ML 01L - Data Exploration Lab)**  | Exploring your data, log-normal distribution, determine baseline metric to beat |
# MAGIC | 30m    | **[Linear Regression I]($./ML 02 - Linear Regression I)**    | Build simple univariate linear regression model<br/> SparkML APIs: transformer vs estimator |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 1 PM
# MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 15m  | **[Linear Regression I Lab]($./Labs/ML 02L - Linear Regression I Lab)**       | Build multivariate linear regression model; Evaluate RMSE and R2 |
# MAGIC | 30m  | **[Linear Regression II]($./ML 03 - Linear Regression II)**      | How to handle categorical variables (OHE)<br/> Pipeline API <br/>Save and load models|
# MAGIC | 30m  | **[MLflow]($./ML 04 - MLflow)** | Use MLflow to track experiments, log metrics, and compare runs| 
# MAGIC | 30m    | **[Decision Trees]($./ML 05 - Decision Trees)**    | Distributed implementation of decision trees and maxBins parameter (why you WILL get different results from sklearn)<br/> Feature importance |
# MAGIC | 30m  | **Break (15:00 - 15:30)**                                               ||
# MAGIC | 30m  | **[Hyperparameter Tuning]($./ML 06 - Hyperparameter Tuning)** | K-Fold cross-validation <br/>SparkML's Parallelism parameter (introduced in Spark 2.3) <br/> Speed up Pipeline model training by 4x | 
# MAGIC | 30m  | **[Hyperparameter Tuning Lab]($./Labs/ML 06L - Hyperparameter Tuning Lab)**  | Perform grid search on a random forest <br/> Get the feature importances across the forest <br/> Save the model <br/> Identify differences between Sklearn's Random Forest and SparkML's |
# MAGIC | 25m  | **[MLeap]($./ML 07 - MLeap)** | Discuss deployment options</br> Export a SparkML model using MLflow + MLeap</br> Make predictions in real-time using MLeap| 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>