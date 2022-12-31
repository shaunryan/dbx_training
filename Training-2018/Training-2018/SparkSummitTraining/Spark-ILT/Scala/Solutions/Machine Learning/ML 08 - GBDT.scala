// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC #Gradient Boosted Decision Trees
// MAGIC <img src="http://insideairbnb.com/images/insideairbnb_graphic_site_1200px.png" style="width:800px"/>
// MAGIC 
// MAGIC The dataset we'll be working with is from Airbnb rentals in San Francisco.<br>
// MAGIC 
// MAGIC You can find more information here:<br>
// MAGIC http://insideairbnb.com/get-the-data.html

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."
// MAGIC 
// MAGIC Install these packages before we start:
// MAGIC  * Only required for the Scala notebook: `databricks:xgboost-linux64:0.8-spark2.3-s_2.11` or use the [Databricks Runtime ML](https://docs.azuredatabricks.net/user-guide/clusters/mlruntime.html) (Distributed XGBoost for Scala pre-installed).
// MAGIC  * `Azure:mmlspark:0.12` (Light GBM). 
// MAGIC  
// MAGIC  Make sure the packages are attached to your cluster. Detach/reattach this notebook to your cluster.

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Recap
// MAGIC 
// MAGIC First, we will get our data ready for predictions and do a quick recap on how to predict prices with Linear Regression

// COMMAND ----------

// MAGIC %md
// MAGIC ###![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Reading the data

// COMMAND ----------

val filePath = "/mnt/training/airbnb/sf-listings/sf-listings-clean.parquet"

val initDF = spark.read.parquet(filePath)

// COMMAND ----------

// MAGIC %md
// MAGIC In the earler labs we preserved the `price_raw` attribute, but we don't need it for modeling. Let's drop it.

// COMMAND ----------

val airbnbDF = initDF.drop("price_raw")
airbnbDF.cache()

// COMMAND ----------

// MAGIC %md
// MAGIC ###![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Data Preparation
// MAGIC 
// MAGIC Before we can apply the linear regression model, we will need to do some data preparation, such as one hot encoding our categorical variables using `StringIndexer` and `OneHotEncoderEstimator`.
// MAGIC 
// MAGIC Let's start by taking a look at all of our columns, and determine which ones are categorical.

// COMMAND ----------

airbnbDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) StringIndexer
// MAGIC 
// MAGIC [Python Docs](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StringIndexer)
// MAGIC 
// MAGIC [Scala Docs](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.StringIndexer)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Now *StringIndex* all categorical features (`neighbourhood_cleansed`, `room_type`, `zipcode`, `property_type`, `bed_type`) and set `handleInvalid` to `skip`.

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer

val iNeighbourhood = new StringIndexer().setInputCol("neighbourhood_cleansed").setOutputCol("cat_neighborhood").setHandleInvalid("skip")
val iRoomType = new StringIndexer().setInputCol("room_type").setOutputCol("cat_room_type").setHandleInvalid("skip")
val iZipCode = new StringIndexer().setInputCol("zipcode").setOutputCol("cat_zip_code").setHandleInvalid("skip")
val iPropertyType = new StringIndexer().setInputCol("property_type").setOutputCol("cat_property_type").setHandleInvalid("skip")
val iBedType= new StringIndexer().setInputCol("bed_type").setOutputCol("cat_bed_type").setHandleInvalid("skip")

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) OneHotEncoder
// MAGIC 
// MAGIC One-hot encode all previously indexed categorical features. We will call the output colums `vec_neighborhood`, `vec_room_type`, `vec_zip_code`, `vec_property_type` and `vec_bed_type`, respectively.

// COMMAND ----------

import org.apache.spark.ml.feature.OneHotEncoderEstimator

val oneHotEnc = new OneHotEncoderEstimator()
oneHotEnc.setInputCols(Array("cat_neighborhood", "cat_room_type", "cat_zip_code", "cat_property_type", "cat_bed_type"))
oneHotEnc.setOutputCols(Array("vec_neighborhood", "vec_room_type", "vec_zip_code", "vec_property_type", "vec_bed_type"))

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Train/Test Split
// MAGIC 
// MAGIC Let's keep 80% for the training set and set aside 20% of our data for the test set.

// COMMAND ----------

val seed = 273
val Array(testDF, trainDF) = airbnbDF.randomSplit(Array(0.20, 0.80), seed=seed)

println(testDF.count, trainDF.count)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Linear Regression Pipeline
// MAGIC 
// MAGIC Let's build the rest of the ML components we'll need in our pipeline, such as `VectorAssembler` and `LinearRegression`.

// COMMAND ----------

val featureCols = Array(
 "host_total_listings_count",
 "accommodates",
 "bathrooms",
 "bedrooms",
 "beds",
 "minimum_nights",
 "number_of_reviews",
 "review_scores_rating",
 "review_scores_accuracy",
 "review_scores_cleanliness",
 "review_scores_checkin",
 "review_scores_communication",
 "review_scores_location",
 "review_scores_value",
 "vec_neighborhood", 
 "vec_room_type", 
 "vec_zip_code", 
 "vec_property_type", 
 "vec_bed_type")

// COMMAND ----------

// MAGIC %md
// MAGIC Set the input columns of the `VectorAssembler` to `featureCols`, the output column to `features` and create a `LinearRegression` that uses the `price` as label. 
// MAGIC 
// MAGIC Docs:
// MAGIC 
// MAGIC  * <a href="https://spark.apache.org/docs/latest/ml-features.html#vectorassembler" target="_blank">VectorAssembler</a>
// MAGIC  * <a href="https://spark.apache.org/docs/latest/ml-classification-regression.html#linear-regression" target="_blank">Linear Regression</a>

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression

val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
val lr = new LinearRegression().setLabelCol("price").setFeaturesCol("features")

// COMMAND ----------

// MAGIC %md
// MAGIC Let's put this all together in a pipeline!
// MAGIC 
// MAGIC Set `iNeighbourhood`, `iRoomType`, `iZipCode`, `iPropertyType`, `iBedType`, `oneHotEnc`, `assembler` and `lr` as the pipeline stages and train a model on the training data:

// COMMAND ----------

import org.apache.spark.ml.Pipeline

val lrPipeline = new Pipeline()

// Now we'll tell the pipeline to first create the feature vector, and then do the linear regression
lrPipeline.setStages(Array(iNeighbourhood, iRoomType, iZipCode, iPropertyType, iBedType, oneHotEnc, assembler, lr))

// Pipelines are themselves Estimators -- so to use them we call fit:
val lrPipelineModel = lrPipeline.fit(trainDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's apply the model to our held-out test set.

// COMMAND ----------

val predictedDF = lrPipelineModel.transform(testDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ###![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Evaluate the Model

// COMMAND ----------

// MAGIC %md
// MAGIC Create a function that displays both *RMSE* and *R2*. We will reuse this many times.

// COMMAND ----------

import org.apache.spark.ml.evaluation.RegressionEvaluator

def printEval(df:org.apache.spark.sql.Dataset[Row], labelCol:String = "price", predictionCol:String = "prediction"):Unit = {
  val evaluator = new RegressionEvaluator()
  evaluator.setLabelCol(labelCol)
  evaluator.setPredictionCol(predictionCol)
  
  val rmse = evaluator.setMetricName("rmse").evaluate(df)
  val r2 = evaluator.setMetricName("r2").evaluate(df)
  println(s"RMSE: $rmse")
  println(s"R2: $r2")
}

// COMMAND ----------

// MAGIC %md
// MAGIC Now, evaluate our model:

// COMMAND ----------

printEval(predictedDF,"price","prediction")

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Log-Normal
// MAGIC Hmmmm... our RMSE was really high. How could we lower it? Let's try converting our `price` target to a logarithmic scale.
// MAGIC 
// MAGIC Let's display the histogram of `log("price")` to confirm that the distribution is log-normal:

// COMMAND ----------

import org.apache.spark.sql.functions._

display(airbnbDF.select(log("price")))

// COMMAND ----------

val logTrainDF = trainDF.withColumn("log_price", log(col("price")))
val logTestDF = testDF.withColumn("log_price", log(col("price")))

// COMMAND ----------

lr.setLabelCol("log_price")
val logPipelineModel = lrPipeline.fit(logTrainDF)

val predictedLogDF = logPipelineModel.transform(logTestDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Exponentiate
// MAGIC 
// MAGIC In order to interpret our RMSE, we need to convert our predictions back from logarithmic scale.

// COMMAND ----------

val expDF = predictedLogDF.withColumn("exp_pred", exp(col("prediction")))
printEval(expDF, "price", "exp_pred")

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) XGBoost
// MAGIC 
// MAGIC Our RMSE decreased significantly from switching to log-normal scale!
// MAGIC 
// MAGIC We could play around with linear regression some more on this dataset, but perhaps it isn't the right algorithm for our dataset (notice the RMSE increases . Let's look at XGBoost (install this Spark package: `databricks:xgboost-linux64:0.8-spark2.3-s_2.11`). XGBoost is one of the most winning Kaggle submission methods.
// MAGIC 
// MAGIC This section is only available in Scala because there is no distributed Python API for XGBoost in Spark yet.

// COMMAND ----------

// MAGIC %scala
// MAGIC // THIS WILL ONLY WORK IF YOU USE THE SCALA VERSION OF THIS NOTEBOOK
// MAGIC 
// MAGIC import ml.dmlc.xgboost4j.scala.spark._
// MAGIC 
// MAGIC val paramMap = List("num_round" -> 100, "nworkers" -> 8, "objective" -> "reg:linear", "eta" -> 0.1, "max_leaf_nodes" -> 50, "early_stopping_rounds" -> 10, "seed" -> 42, "labelCol" -> "log_price").toMap
// MAGIC 
// MAGIC val xgboostEstimator = new XGBoostEstimator(paramMap)
// MAGIC 
// MAGIC val xgboostPipeline = new Pipeline().setStages(Array(iNeighbourhood, iRoomType, iZipCode, iPropertyType, iBedType, oneHotEnc, assembler, xgboostEstimator))
// MAGIC 
// MAGIC val xgboostPipelineModel = xgboostPipeline.fit(logTrainDF)
// MAGIC val xgboostLogPredictedDF = xgboostPipelineModel.transform(logTestDF)
// MAGIC 
// MAGIC val expXgboostDF = xgboostLogPredictedDF.withColumn("exp_pred", exp(col("prediction")))
// MAGIC 
// MAGIC printEval(expXgboostDF, "price", "exp_pred")
// MAGIC println("-" * 80)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Light GBM
// MAGIC Perhaps try a different algorithm? Let's look at Light GBM (install this Spark package: `Azure:mmlspark:0.12`). Light GBM is an alternative gradient boosting technique to XGBoost to significantly speed up the computation.

// COMMAND ----------

import com.microsoft.ml.spark.LightGBMRegressor

val gbmModel = new LightGBMRegressor()
                    .setLearningRate(0.1)
                    .setNumIterations(100)
                    .setNumLeaves(50)
                    .setLabelCol("log_price")

val gbmPipeline = new Pipeline()
gbmPipeline.setStages(Array(iNeighbourhood, iRoomType, iZipCode, iPropertyType, iBedType, oneHotEnc, assembler, gbmModel))

val gbmPipelineModel = gbmPipeline.fit(logTrainDF)
val gbmLogPredictedDF = gbmPipelineModel.transform(logTestDF)

val expGbmDF = gbmLogPredictedDF.withColumn("exp_pred", exp(col("prediction")))
printEval(expGbmDF, "price", "exp_pred")
println("-" * 80)

// COMMAND ----------

// MAGIC %md
// MAGIC Wow! The gradient boosted trees did much better than linear regression!
// MAGIC 
// MAGIC Go back through this notebook and try to see how low you can get the RMSE!

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>