# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #Linear Regression Lab with Airbnb
# MAGIC <img src="http://insideairbnb.com/images/insideairbnb_graphic_site_1200px.png" style="width:800px"/>
# MAGIC 
# MAGIC The dataset we'll be working with is from Airbnb rentals in San Francisco<br>
# MAGIC 
# MAGIC You can find more information here:<br>
# MAGIC http://insideairbnb.com/get-the-data.html

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Reading the data

# COMMAND ----------

filePath = "/mnt/training/airbnb/sf-listings/sf-listings-clean.parquet"

initDF = spark.read.parquet(filePath)

# COMMAND ----------

display(initDF.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC In the previous lab we preserved the `price_raw` attribute, but we don't need it for modeling. Let's drop it.

# COMMAND ----------

airbnbDF = initDF.drop("price_raw")
airbnbDF.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's make sure we don't have any null values in our DataFrame

# COMMAND ----------

recordCount = airbnbDF.count()
noNullsRecordCount = airbnbDF.na.drop().count()

print("We have {} records that contain null values.".format(recordCount - noNullsRecordCount))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Exploratory data analysis

# COMMAND ----------

# MAGIC %md
# MAGIC First, create a view calles `airbnb` from our dataset so you can move on using both the DataFrame or the SQL API.

# COMMAND ----------

airbnbDF.createOrReplaceTempView("airbnb")

# COMMAND ----------

# MAGIC %md
# MAGIC Let's make a histogram of the price column to explore it (change the number of bins to 300).  

# COMMAND ----------

# ANSWER
display(airbnbDF.select("price"))

# COMMAND ----------

# MAGIC %md
# MAGIC Is this a <a href="https://en.wikipedia.org/wiki/Log-normal_distribution" target="_blank">Log Normal</a> distribution? Take the `log` of price and check the histogram.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import *

display(airbnbDF.select(log("price")))

# COMMAND ----------

# MAGIC %md
# MAGIC Now take a look at how `price` depends on some of the variables:
# MAGIC * Plot `price` vs `bedrooms`
# MAGIC * Plot `price` vs `accomodates`

# COMMAND ----------

# ANSWER
display(airbnbDF)

# COMMAND ----------

# ANSWER
display(airbnbDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at the distribution of some of our categorical features

# COMMAND ----------

display(airbnbDF.groupBy("room_type").count())

# COMMAND ----------

# MAGIC %md
# MAGIC Which neighborhoods have the highest number of rentals? Display the neighbourhoods and their associated count in descending order.

# COMMAND ----------

# ANSWER
display(airbnbDF.groupBy("neighbourhood_cleansed").count().orderBy(col("count").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### How much does the price depend on the location?

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import col
# MAGIC mapDF = spark.table("airbnb")
# MAGIC v = ",\n".join(map(lambda row: "[{}, {}, {}]".format(row[0], row[1], row[2]), mapDF.select(col("latitude"),col("longitude"),col("price")/600).collect()))
# MAGIC displayHTML("""
# MAGIC <html>
# MAGIC <head>
# MAGIC  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.3.1/dist/leaflet.css"
# MAGIC    integrity="sha512-Rksm5RenBEKSKFjgI3a41vrjkw4EVPlJ3+OiI65vTjIdo9brlAacEuKOiQ5OFh7cOI1bkDwLqdLw3Zg0cRJAAQ=="
# MAGIC    crossorigin=""/>
# MAGIC  <script src="https://unpkg.com/leaflet@1.3.1/dist/leaflet.js"
# MAGIC    integrity="sha512-/Nsx9X4HebavoBvEBuyp3I7od5tA0UzAxs+j83KgC8PU0kgB4XiK4Lfe4y4cgBtaRJQEIFCW+oC506aPT2L1zw=="
# MAGIC    crossorigin=""></script>
# MAGIC  <script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet.heat/0.2.0/leaflet-heat.js"></script>
# MAGIC </head>
# MAGIC <body>
# MAGIC     <div id="mapid" style="width:700px; height:500px"></div>
# MAGIC   <script>
# MAGIC   var mymap = L.map('mapid').setView([37.7587,-122.4486], 12);
# MAGIC   var tiles = L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
# MAGIC     attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors',
# MAGIC }).addTo(mymap);
# MAGIC   var heat = L.heatLayer([""" + v + """], {radius: 25}).addTo(mymap);
# MAGIC   </script>
# MAGIC   </body>
# MAGIC   </html>
# MAGIC """)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Train a Linear Regression Model
# MAGIC 
# MAGIC Before we can apply the linear regression model, we will need to do some data preparation, such as one hot encoding our categorical variables using `StringIndexer` and `OneHotEncoderEstimator`.
# MAGIC 
# MAGIC Let's start by taking a look at all of our columns, and determine which ones are categorical.

# COMMAND ----------

airbnbDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) StringIndexer
# MAGIC 
# MAGIC [Python Docs](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StringIndexer)
# MAGIC 
# MAGIC [Scala Docs](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.StringIndexer)

# COMMAND ----------

from pyspark.ml.feature import StringIndexer

print(StringIndexer().explainParams())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now *StringIndex* all categorical features (`neighbourhood_cleansed`, `room_type`, `zipcode`, `property_type`, `bed_type`) and set `handleInvalid` to `skip`. Set the output columns to `cat_neighbourhood_cleansed`, `cat_room_type`, `cat_zipcode`, `cat_property_type` and `cat_bed_type`, respectively.

# COMMAND ----------

# ANSWER
iNeighbourhood = StringIndexer(inputCol="neighbourhood_cleansed", outputCol="cat_neighborhood", handleInvalid="skip")
iRoomType = StringIndexer(inputCol="room_type", outputCol="cat_room_type", handleInvalid="skip")
iZipCode = StringIndexer(inputCol="zipcode", outputCol="cat_zipcode", handleInvalid="skip")
iPropertyType = StringIndexer(inputCol="property_type", outputCol="cat_property_type", handleInvalid="skip")
iBedType= StringIndexer(inputCol="bed_type", outputCol="cat_bed_type", handleInvalid="skip")

# COMMAND ----------

# MAGIC %md
# MAGIC Test your solution:

# COMMAND ----------

assert iNeighbourhood.getOutputCol() == "cat_neighborhood", "iNeighbourhood: Expected output column 'cat_neighborhood', got '" + iNeighbourhood.getOutputCol() + "'"
assert iRoomType.getOutputCol() == "cat_room_type", "iRoomType: Expected output column 'cat_room_type', got '" + iRoomType.getOutputCol() + "'"
assert iZipCode.getOutputCol() == "cat_zipcode", "iZipCode: Expected output column 'cat_zipcode', got '" + iRoomType.getOutputCol() + "'"
assert iPropertyType.getOutputCol() == "cat_property_type", "iPropertyType: Expected output column 'cat_property_type', got '" + iRoomType.getOutputCol() + "'"
assert iBedType.getOutputCol() == "cat_bed_type", "iBedType: Expected output column 'cat_bed_type', got '" + iRoomType.getOutputCol() + "'"
print("Tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) OneHotEncoder
# MAGIC 
# MAGIC One-hot encode all previously indexed categorical features. Call the output colums `vec_neighborhood`, `vec_room_type`, `vec_zipcode`, `vec_property_type` and `vec_bed_type`, respectively.

# COMMAND ----------

# ANSWER
from pyspark.ml.feature import OneHotEncoderEstimator

oneHotEnc = OneHotEncoderEstimator()
oneHotEnc.setInputCols(["cat_neighborhood", "cat_room_type", "cat_zipcode", "cat_property_type", "cat_bed_type"])
oneHotEnc.setOutputCols(["vec_neighborhood", "vec_room_type", "vec_zipcode", "vec_property_type", "vec_bed_type"])

# COMMAND ----------

# MAGIC %md
# MAGIC Test your solution:

# COMMAND ----------

assert set(oneHotEnc.getInputCols()) == set(["cat_neighborhood", "cat_room_type", "cat_zipcode", "cat_property_type", "cat_bed_type"]), 'oneHotEnc expected inputCols: "cat_neighborhood", "cat_room_type", "cat_zipcode", "cat_property_type", "cat_bed_type"'
        
assert set(oneHotEnc.getOutputCols()) == set(["vec_neighborhood", "vec_room_type", "vec_zipcode", "vec_property_type", "vec_bed_type"]), 'oneHotEnc expected outputCols: "vec_neighborhood", "vec_room_type", "vec_zipcode", "vec_property_type", "vec_bed_type"'

print("Tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Train/Test Split
# MAGIC 
# MAGIC Let's keep 80% for the training set and set aside 20% of our data for the test set.

# COMMAND ----------

# ANSWER
seed = 273
(testDF, trainDF) = airbnbDF.randomSplit((0.20, 0.80), seed=seed)

print(testDF.count(), trainDF.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Pipeline
# MAGIC 
# MAGIC Let's build some of the transformations we'll need in our pipeline, such as `VectorAssembler` and `LinearRegression`.

# COMMAND ----------

featureCols = [
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
 "vec_zipcode", 
 "vec_property_type", 
 "vec_bed_type"]

# COMMAND ----------

# MAGIC %md
# MAGIC Set the input columns of the `VectorAssembler` to `featureCols`, the output column to `features` and create a `LinearRegression` that uses the `price` as label. :
# MAGIC 
# MAGIC  * <a href="https://spark.apache.org/docs/latest/ml-features.html#vectorassembler" target="_blank">VectorAssembler Docs</a>
# MAGIC  * <a href="https://spark.apache.org/docs/latest/ml-classification-regression.html#linear-regression" target="_blank">Linear Regression Docs</a>

# COMMAND ----------

# ANSWER

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

assembler = VectorAssembler(inputCols=featureCols, outputCol="features")

lr = (LinearRegression()
      .setLabelCol("price")
      .setFeaturesCol("features"))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's put this all together in a pipeline!
# MAGIC 
# MAGIC Set `iNeighbourhood`, `iRoomType`, `iZipCode`, `iPropertyType`, `iBedType`, `oneHotEnc`, `assembler` and `lr` as the pipeline stages and train a model on the training data:

# COMMAND ----------

# ANSWER
from pyspark.ml import Pipeline

lrPipeline = Pipeline()

# Now we'll tell the pipeline to first create the feature vector, and then do the linear regression
lrPipeline.setStages([iNeighbourhood, iRoomType, iZipCode, iPropertyType, iBedType, oneHotEnc, assembler, lr])

# Pipelines are themselves Estimators -- so to use them we call fit:
lrPipelineModel = lrPipeline.fit(trainDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Test your solution:

# COMMAND ----------

assert len(lrPipeline.getStages()) == 8, "Expected 8 stages in the pipeline. 5 StringIndexers, the OneHotEncoredEstimator, the VectorAssembler and the Linear Regression"

print("Tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC Let's apply the model to our held-out test set.

# COMMAND ----------

# ANSWER
predictedDF = lrPipelineModel.transform(testDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Evaluate the Model

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator()
print(evaluator.explainParams())

# COMMAND ----------

evaluator.setLabelCol("price")
evaluator.setPredictionCol("prediction")

metricName = evaluator.getMetricName()
metricVal = evaluator.evaluate(predictedDF)

print("{}: {}".format(metricName, metricVal))

# COMMAND ----------

# MAGIC %md
# MAGIC We could wrap this into a function to make it easier to get the output of multiple metrics.

# COMMAND ----------

def printEval(df, labelCol = "price", predictionCol = "prediction"):
  evaluator = RegressionEvaluator()
  evaluator.setLabelCol(labelCol)
  evaluator.setPredictionCol(predictionCol)

  rmse = evaluator.setMetricName("rmse").evaluate(df)
  r2 = evaluator.setMetricName("r2").evaluate(df)
  print("RMSE: {}\nR2: {}".format(rmse, r2))

# COMMAND ----------

printEval(predictedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Conclusion
# MAGIC Hmmmm... our RMSE was really high. How could we lower it? You will see some techniques in the next notebook.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>