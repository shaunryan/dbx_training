// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Classification: Logistic Regression
// MAGIC 
// MAGIC Up until this point, we have only examined regression use cases. Now let's take a look at how to handle classification.
// MAGIC 
// MAGIC For this lab, we will use the same Airbnb dataset, but instead of predicting price, we will predict if host is a [superhost](https://www.airbnb.com/superhost) or not in San Francisco.
// MAGIC 
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
// MAGIC  - Build a Logistic Regression model
// MAGIC  - Use various metrics to evaluate model performance

// COMMAND ----------

// MAGIC %run "../Includes/Classroom-Setup"

// COMMAND ----------

val filePath = "dbfs:/mnt/training/airbnb/sf-listings/sf-listings-2019-03-06-clean.parquet/"
val airbnbDF = spark.read.parquet(filePath)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Baseline Model
// MAGIC 
// MAGIC Before we build any Machine Learning models, we want to build a baseline model to compare to. We are going to start by predicting if a host is a [superhost](https://www.airbnb.com/superhost). 
// MAGIC 
// MAGIC For our baseline model, we are going to predict no on is a superhost and evaluate our accuracy. We will examine other metrics later as we build more complex models.
// MAGIC 
// MAGIC 0. Convert our `host_is_superhost` column (t/f) into 1/0 and call the resulting column `label`. DROP the `host_is_superhost` afterwards.
// MAGIC 0. Add a column to the resulting DataFrame called `prediction` which contains the literal value `0.0`. We will make a constant prediction that no one is a superhost.
// MAGIC 
// MAGIC After we finish these two steps, then we can evaluate the "model" accuracy. 
// MAGIC 
// MAGIC Some helpful functions:
// MAGIC * when() [Python](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.when)/[Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.functions$)
// MAGIC * withColumn() [Python](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.withColumn)/[Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.Dataset)
// MAGIC * lit() [Python](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.lit)/[Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.functions$)

// COMMAND ----------

// TODO
import <FILL_IN>

val labelDF = airbnbDF.<FILL_IN>

val predDF = labelDF.<FILL_IN> // Add a prediction column

// COMMAND ----------

// MAGIC %md
// MAGIC ## Evaluate model
// MAGIC 
// MAGIC For right now, let's use accuracy as our metric. This is available from MulticlassClassificationEvaluator [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.evaluation.MulticlassClassificationEvaluator)/[Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator).

// COMMAND ----------

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

val mcEvaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
println(s"The accuracy is ${100*mcEvaluator.evaluate(predDF)}%")
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Train-Test Split
// MAGIC 
// MAGIC Alright! Now we have built a baseline model. The next step is to split our data into a train-test split.

// COMMAND ----------

val Array(trainDF, testDF) = labelDF.randomSplit(Array(.8, .2), seed=42)
println(trainDF.cache().count)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Visualize
// MAGIC 
// MAGIC Let's look at the relationship between `review_scores_rating` and `label` in our training dataset.

// COMMAND ----------

display(trainDF.select("review_scores_rating", "label"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Logistic Regression
// MAGIC 
// MAGIC Now build a logistic regression model ([Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.LogisticRegression)/[Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.classification.LogisticRegression)) using all of the features (HINT: use RFormula). Put the pre-processing step and the Logistic Regression Model into a Pipeline.

// COMMAND ----------

// TODO
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.classification.LogisticRegression

val rFormula = new RFormula().<FILL_IN>
val lr = <FILL_IN>
val pipeline = new Pipeline().<FILL_IN>
val pipelineModel = pipeline.fit(<FILL_IN>)
val predDF = pipelineModel.transform(<FILL_IN>)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Evaluate
// MAGIC 
// MAGIC What is AUROC useful for? Try adding additional evalution metrics, like Area Under PR Curve.

// COMMAND ----------

// TODO
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, BinaryClassificationEvaluator}

val mcEvaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
println(f"The accuracy is ${100*mcEvaluator.evaluate(predDF)}%1.2f%%")

val bcEvaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")
println(f"The area under the ROC curve: ${bcEvaluator.evaluate(predDF)}%1.2f")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Add Hyperparameter Tuning
// MAGIC 
// MAGIC Try changing the hyperparameters of the logistic regression model using the cross-validator. By how much can you improve your metrics? 

// COMMAND ----------

// TODO
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.CrossValidator

val paramGrid = <FILL_IN>

val evaluator = <FILL_IN>

val cv = <FILL_IN>

val pipeline = <FILL_IN>

val pipelineModel = <FILL_IN>

val predDF = <FILL_IN>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Evaluate again

// COMMAND ----------

val mcEvaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
println(f"The accuracy is ${100*mcEvaluator.evaluate(predDF)}%1.2f%%")

val bcEvaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")
println(f"The area under the ROC curve: ${bcEvaluator.evaluate(predDF)}%1.2f")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Super Bonus
// MAGIC 
// MAGIC Try using MLflow to track your experiments!

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>