// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC # Gradient Boosting
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Introduction
// MAGIC 
// MAGIC Gradient boosting (GBM) is a highly-successful ensemble technique for machine learning. An ensemble is a collection of models that work together to give a final prediction. 
// MAGIC 
// MAGIC There are many types of ensembles, such as:
// MAGIC 
// MAGIC  * Bagging: We build many *independent* models and combine them with an averging technique to give a final prediction. <a href="https://spark.apache.org/docs/latest/ml-classification-regression.html#random-forests">Random Forests</a> is such a technique.
// MAGIC  * **Boosting**: We build many models *sequentially* where each new model created fixes some of the weaknesses of the preceding model.
// MAGIC  
// MAGIC **Gradient Boosted Trees (GBT)** is an ensemble technique that combines Decision Trees with the *Boosting* technique. 
// MAGIC 
// MAGIC For more information check out the <a href="https://en.wikipedia.org/wiki/Gradient_boosting">Wikipedia article</a> on Gradient Boosting.
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/gradient-boosting.png"/>

// COMMAND ----------

// MAGIC %md
// MAGIC INSTRUCTOR_NOTE
// MAGIC 
// MAGIC We jump between scala and python in this notebook. The reason is that we got to use numpy to generate our demo data. But we only have an XGBoost implementation in Scala at the moment.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Our Dataset

// COMMAND ----------

// MAGIC %md
// MAGIC Let's create a random dataset and visualize *x* vs *y* on a Scatter plot:

// COMMAND ----------

// MAGIC %python
// MAGIC import numpy as np
// MAGIC np.random.seed(273)
// MAGIC 
// MAGIC y = np.concatenate((
// MAGIC   np.random.uniform(0,5,100),
// MAGIC   np.random.uniform(94,100,100),
// MAGIC   np.random.uniform(40,45,100),
// MAGIC   np.random.uniform(10,17,100)
// MAGIC   )).tolist()
// MAGIC 
// MAGIC df = spark.createDataFrame(list(zip(range(0,len(y)),y)), ["x","y"])
// MAGIC display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC Create a feature vector from *x*, split the data into training and test, and create two views, `training` and `test`:

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.ml.feature import VectorAssembler
// MAGIC 
// MAGIC assembler = VectorAssembler(inputCols=["x"], outputCol="features")
// MAGIC featureLabelDF = assembler.transform(df.withColumnRenamed("y","label")).select("x","features","label")
// MAGIC 
// MAGIC (trainingDF, testDF) = featureLabelDF.randomSplit([0.7,0.3])
// MAGIC trainingDF.createOrReplaceTempView("training")
// MAGIC testDF.createOrReplaceTempView("test")
// MAGIC 
// MAGIC display(trainingDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) GBTRegressor
// MAGIC Documentation:
// MAGIC 
// MAGIC  * <a href='https://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-regression' target="_blank">Gradient Boosted Tree Regression documentation</a> in the Spark ML programming guide

// COMMAND ----------

// MAGIC %md
// MAGIC Import the *GBTRegressor* class and take a look at the its parameters:

// COMMAND ----------

import org.apache.spark.ml.regression.GBTRegressor
val gbt = new GBTRegressor()

gbt.explainParams()

// COMMAND ----------

// MAGIC %md
// MAGIC Make a predictions with 5 trees and display a Line chart of *x* vs *prediction*:

// COMMAND ----------

val trainingDF = spark.table("training")
val testDF = spark.table("test")

val gbtModel = gbt.setSeed(273).setMaxIter(5).fit(trainingDF)
val predictedDF = gbtModel.transform(testDF)

predictedDF.createOrReplaceTempView("predictions")
display(predictedDF.select("x","prediction"))

// COMMAND ----------

// MAGIC %md
// MAGIC Take a look at the performance of our GBT Model:

// COMMAND ----------

import org.apache.spark.ml.evaluation.RegressionEvaluator

val eval = new RegressionEvaluator()
println("%s: %s".format(
  eval.getMetricName,
  eval.evaluate(predictedDF)
))
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's visualize our test labels against the predictions:

// COMMAND ----------

// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC 
// MAGIC x, labels, predictions = zip(*spark.table("predictions").select("x","label","prediction").collect())
// MAGIC 
// MAGIC fig, ax = plt.subplots(figsize=(5,3))
// MAGIC ax.set_xlim(-30,420)
// MAGIC ax.set_ylim(-15,115)
// MAGIC ax.plot(x,labels,'o', markersize=6, color="orange")
// MAGIC ax.plot(x, predictions, markersize=0.1)
// MAGIC 
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) XGBoost
// MAGIC 
// MAGIC <a href="https://github.com/dmlc/xgboost" target="_blank">XGBoost</a> is an independent GBT implemetation. It's one of the most winning Kaggle submission methods. It's a high-performance distributed GBM implementation.
// MAGIC 
// MAGIC Make sure you installed this Spark package: `databricks:xgboost-linux64:0.8-spark2.3-s_2.11`. 
// MAGIC 
// MAGIC This section is only available in Scala because there is no distributed Python API for XGBoost in Spark yet.
// MAGIC 
// MAGIC Here we are creating an XGBoost model of a maximum of 100 decision trees:

// COMMAND ----------

// MAGIC %scala
// MAGIC import ml.dmlc.xgboost4j.scala.spark._
// MAGIC 
// MAGIC val trainingDF = spark.table("training")
// MAGIC val testDF = spark.table("test")
// MAGIC 
// MAGIC val paramMap = List("num_round" -> 100, "nworkers" -> 8, "objective" -> "reg:linear", "eta" -> 0.1, "max_leaf_nodes" -> 50, "seed" -> 42).toMap
// MAGIC 
// MAGIC val xgboostEstimator = new XGBoostEstimator(paramMap)
// MAGIC val xgboostPipelineModel = xgboostEstimator.fit(trainingDF)
// MAGIC val predictedDF = xgboostPipelineModel.transform(testDF)
// MAGIC 
// MAGIC predictedDF.createOrReplaceTempView("xgboost_predictions")

// COMMAND ----------

// MAGIC %md
// MAGIC Let's see how this compares to our original GBTRegressor model

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.ml.evaluation.RegressionEvaluator
// MAGIC 
// MAGIC val eval = new RegressionEvaluator()
// MAGIC println("%s: %s".format(
// MAGIC   eval.getMetricName,
// MAGIC   eval.evaluate(predictedDF)
// MAGIC ))
// MAGIC println("-"*80)

// COMMAND ----------

// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC 
// MAGIC x, labels, predictions = zip(*spark.table("xgboost_predictions").select("x","label","prediction").collect())
// MAGIC 
// MAGIC fig, ax = plt.subplots(figsize=(5,3))
// MAGIC ax.set_xlim(-30,420)
// MAGIC ax.set_ylim(-15,115)
// MAGIC ax.plot(x,labels,'o', markersize=6, color="orange")
// MAGIC ax.plot(x, predictions, markersize=0.1)
// MAGIC 
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>