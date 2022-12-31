# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img src="https://files.training.databricks.com/images/movie-camera.png" style="float:right; height: 200px; margin: 10px; border: 1px solid #ddd; border-radius: 15px 15px 15px 15px; padding: 10px"/>
# MAGIC 
# MAGIC # Movie Recommendations
# MAGIC 
# MAGIC In the previous labs, we didn't need to do any data preprocessing. In this lab, we will use our preprocessing steps from Spark as input to Horovod. 
# MAGIC 
# MAGIC Here, we will use 1 million movie ratings from the [MovieLens stable benchmark rating dataset](http://grouplens.org/datasets/movielens/). We will start by building a benchmark model with ALS, and then see if we can beat that benchmark with a neural network!
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC - Combine User + Item factors identified from ALS and use as input to a neural network
# MAGIC - Create custom activation function (scaled sigmoid) to bound output of regression tasks
# MAGIC - Train distributed neural network using Horovod
# MAGIC  

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

moviesDF = spark.read.parquet("dbfs:/mnt/training/movielens/movies.parquet/")
ratingsDF = spark.read.parquet("dbfs:/mnt/training/movielens/ratings.parquet/")

ratingsDF.cache()
moviesDF.cache()

ratingsCount = ratingsDF.count()
moviesCount = moviesDF.count()

print(f"There are {ratingsCount} ratings and {moviesCount} movies in the datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a quick look at some of the data in the two DataFrames.

# COMMAND ----------

display(moviesDF)

# COMMAND ----------

display(ratingsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC What range of values do the ratings take?

# COMMAND ----------

# Double check range of values
import pyspark.sql.functions as F
display(ratingsDF.select("rating").describe())

# COMMAND ----------

# MAGIC %md
# MAGIC Let's start by splitting our data into a training and test set.

# COMMAND ----------

seed=42
(trainingDF, testDF) = ratingsDF.randomSplit([0.8, 0.2], seed=seed)

print(f"Training: {trainingDF.count()}, test: {testDF.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternating Least Squares Method (ALS)
# MAGIC 
# MAGIC ALS is a parallel algorithm for matrix factorization. Taking the (often low rank) matrix of every user's rating of every movie, ALS will try to find 2 lower dimensional matrices whose product will approximate the original matrix. One matrix represents users and their latent factors while the other contains movies and their latent factors. 
# MAGIC 
# MAGIC Since our goal is to be able to predict every user's rating of every movie, knowing these 2 lower dimensional matrices will give us what we need to reconstruct this information.
# MAGIC 
# MAGIC ![factorization](http://spark-mooc.github.io/web-assets/images/matrix_factorization.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's build and train our baseline ALS model.

# COMMAND ----------

from pyspark.ml.recommendation import ALS

als = (ALS()
       .setUserCol("userId")
       .setItemCol("movieId")
       .setRatingCol("rating")
       .setPredictionCol("prediction")
       .setMaxIter(3)
       .setSeed(seed)
       .setRegParam(0.1)
       .setColdStartStrategy("drop")
       .setRank(12)
       .setNonnegative(True))

alsModel = als.fit(trainingDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Evaluate the model on the test data by looking at the mean squared error of our predictions on test data.

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

regEval = RegressionEvaluator(predictionCol="prediction", labelCol="rating", metricName="mse")

predictedTestDF = alsModel.transform(testDF)

testMse = regEval.evaluate(predictedTestDF)

print(f"The model had a MSE on the test set of {testMse}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deep Learning
# MAGIC 
# MAGIC Now let's take a deep learning approach to predicting the rating values.
# MAGIC 
# MAGIC Let's take the latent factors learned from ALS and include them as features! The following cell extracts the user and item (movie) features from the trained ALS model and joins them in our train and test DataFrames.

# COMMAND ----------

userFactors = alsModel.userFactors.selectExpr("id as userId", "features as uFeatures")
itemFactors = alsModel.itemFactors.selectExpr("id as movieId", "features as iFeatures")
joinedTrainDF = trainingDF.join(itemFactors, on="movieId").join(userFactors, on="userId")
joinedTestDF = testDF.join(itemFactors, on="movieId").join(userFactors, on="userId")

# COMMAND ----------

display(joinedTrainDF)

# COMMAND ----------

# MAGIC %md
# MAGIC We would like to use the `iFeatures` and `uFeatures` columns as input for our deep learning model. However, we need all our features to be in one column of our DataFrame.
# MAGIC 
# MAGIC The code below creates two new DataFrames, `concatTrainDF` and `concatTestDF`, with the following three columns: `userId`, `movieId`, and `features` which contains the concatenated `iFeatures` and `uFeatures` arrays. 

# COMMAND ----------

from itertools import chain
from pyspark.sql.functions import *
from pyspark.sql.types import *

def concat_arrays(*args):
    return list(chain(*args))
    
concat_arrays_udf = udf(concat_arrays, ArrayType(FloatType()))

concatTrainDF = joinedTrainDF.select("userId", "movieId", concat_arrays_udf(col("iFeatures"), col("uFeatures")).alias("features"), "rating")

concatTestDF = joinedTestDF.select("userId", "movieId", concat_arrays_udf(col("iFeatures"), col("uFeatures")).alias("features"), "rating")

# COMMAND ----------

# MAGIC %md
# MAGIC Check that your DataFrame has the correct columms.

# COMMAND ----------

display(concatTrainDF.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Petastorm
# MAGIC 
# MAGIC Prepare data for Petastorm.

# COMMAND ----------

train_path = userhome + "/deep-learning/ALS_train.parquet"
test_path = userhome + "/deep-learning/ALS_test.parquet"

concatTrainDF.selectExpr("features", "rating as label").repartition(8).write.mode("overwrite").parquet(train_path)
concatTestDF.selectExpr("features", "rating as label").repartition(8).write.mode("overwrite").parquet(test_path)

# COMMAND ----------

# MAGIC %md
# MAGIC We have to remove any of the started/committed files that Spark automatically writes when it saves to Parquet.

# COMMAND ----------

[dbutils.fs.rm(i.path) for i in dbutils.fs.ls(train_path) if ("_committed_" in i.name) | ("_started_" in i.name)]
[dbutils.fs.rm(i.path) for i in dbutils.fs.ls(test_path) if ("_committed_" in i.name) | ("_started_" in i.name)]

display(dbutils.fs.ls(train_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Model and HorovodRunner
# MAGIC 
# MAGIC We'll create two models, a baseline model that uses a linear activation function at the end, and one with a scaled [sigmoid](https://www.tensorflow.org/api_docs/python/tf/keras/backend/sigmoid) function whose outputs are bounded 0.5 to 5 (range of review scores).

# COMMAND ----------

from sparkdl import HorovodRunner
import tensorflow as tf
tf.set_random_seed(42) # For reproducibility
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
import numpy as np
from tensorflow.keras import backend as K

def build_model():
  return Sequential([Dense(30, input_shape=(24,), activation='relu'),
                    Dense(20, activation='relu'),
                    Dense(1, activation='linear')])


def sigmoid_activation(x): # Scores range from 0.5 to 5
  return (K.sigmoid(x) * 4.5) + .5

def build_model_sigmoid():
  return Sequential([Dense(30, input_shape=(24,), activation='relu'),
                    Dense(20, activation='relu'),
                    Dense(1, activation=sigmoid_activation)])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tensorboard
# MAGIC We will set up Tensorboard so we can visualize and debug our network. Take a look at the [dbutils.tensorboard.start](https://docs.azuredatabricks.net/applications/deep-learning/single-node-training/tensorflow.html#tensorboard) function. 
# MAGIC 
# MAGIC NOTE: It will not start logging until we start training our model.

# COMMAND ----------

dbutils.fs.rm(f"{ml_username}/tensorboard", True)
log_dir = f"/dbfs/{ml_username}/tensorboard"
dbutils.tensorboard.start(log_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### HorvodRunner with Petastorm

# COMMAND ----------

from tensorflow.keras import optimizers
from tensorflow.keras.callbacks import *
import shutil
import horovod.tensorflow.keras as hvd
from petastorm import make_batch_reader
from petastorm.tf_utils import make_petastorm_dataset

abs_file_path = train_path.replace("dbfs:/", "/dbfs/")
def run_training_horovod_petastorm(model_type = "default"):
  # Horovod: initialize Horovod.
  hvd.init()
  print(f"Rank is: {hvd.rank()}")
  print(f"Size is: {hvd.size()}")
  with make_batch_reader("file://" + abs_file_path, num_epochs=None, cur_shard=hvd.rank(), shard_count= hvd.size()) as reader:
    dataset = make_petastorm_dataset(reader).map(lambda x: (tf.reshape(x.features, [-1,24]), tf.reshape(x.label, [-1,1])))  
    if model_type == "default":
      model = build_model()
    else:
      model = build_model_sigmoid()

    # Horovod: adjust learning rate based on number of CPUs/GPUs.
    optimizer = optimizers.Adam(lr=0.001*hvd.size())

    # Horovod: add Horovod Distributed Optimizer.
    optimizer = hvd.DistributedOptimizer(optimizer)

    model.compile(optimizer=optimizer, loss="mse", metrics=["mse"])

    # Use the optimized FUSE Mount
    checkpoint_dir = f"/dbfs/ml/{ml_username}/{model_type}_horovod_checkpoint_weights.ckpt"

    callbacks = [
      hvd.callbacks.BroadcastGlobalVariablesCallback(0),
      hvd.callbacks.MetricAverageCallback(),
      hvd.callbacks.LearningRateWarmupCallback(warmup_epochs=5, verbose=1),
      ReduceLROnPlateau(monitor="loss", patience=10, verbose=1)
    ]

    # Horovod: save checkpoints only on worker 0 to prevent other workers from corrupting them.
    if hvd.rank() == 0:
      callbacks.append(tf.keras.callbacks.ModelCheckpoint(checkpoint_dir, save_weights_only = True))
      callbacks.append(tf.keras.callbacks.TensorBoard(f"{log_dir}/petastorm_{model_type}"))

    history = model.fit(dataset, callbacks=callbacks, steps_per_epoch=200, epochs=3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train and Evaluate Model with Linear Activation

# COMMAND ----------

hr = HorovodRunner(np=-1) # using all workers is very slow
hr.run(run_training_horovod_petastorm, model_type="default")

# COMMAND ----------

# MAGIC %md
# MAGIC Load in saved model + test data

# COMMAND ----------

import pandas as pd

testDF = pd.read_parquet(test_path.replace("dbfs:/", "/dbfs/"))
X_test = np.array([np.array(xi) for xi in testDF["features"].values]) # Reshape values
y_test = testDF["label"]

# COMMAND ----------

# MAGIC %md
# MAGIC Let's evaluate and compare to how we did with vanilla ALS.

# COMMAND ----------

model = build_model()
model_type = "default"
weights_path = f"/dbfs/ml/{ml_username}/{model_type}_horovod_checkpoint_weights.ckpt"
model.load_weights(weights_path)
# TF requires to recompile
optimizer = optimizers.Adam(lr=0.001)
model.compile(optimizer=optimizer, loss="mse", metrics=["mse"])
# Get MSE loss of model
loss, _ = model.evaluate(X_test, y_test)
print(f"Restored default model, loss {loss}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train and Evaluate Model with Scaled Sigmoid
# MAGIC 
# MAGIC Now train the model with the scaled sigmoid activation function by setting `model_type` to `sigmoid`.

# COMMAND ----------

hr = HorovodRunner(np=-1) # using all workers is very slow
hr.run(run_training_horovod_petastorm, model_type="sigmoid")

# COMMAND ----------

# MAGIC %md
# MAGIC How much better did the scaled sigmoid do?

# COMMAND ----------

model = build_model_sigmoid()
model_type = "sigmoid"
weights_path = f"/dbfs/ml/{ml_username}/{model_type}_horovod_checkpoint_weights.ckpt"
model.load_weights(weights_path)
# TF requires to recompile
optimizer = optimizers.Adam(lr=0.001)
model.compile(optimizer=optimizer, loss="mse", metrics=["mse"])
# Get MSE loss of model
loss, _ = model.evaluate(X_test, y_test)
print(f"Restored sigmoid model, loss {loss}")


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>