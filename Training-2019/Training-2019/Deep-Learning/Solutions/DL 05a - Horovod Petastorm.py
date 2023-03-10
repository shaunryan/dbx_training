# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Horovod with Petastorm
# MAGIC 
# MAGIC [Petastorm](https://github.com/uber/petastorm) enables single machine or distributed training and evaluation of deep learning models from datasets in Apache Parquet format. It supports ML frameworks such as Tensorflow, Pytorch, and PySpark and can be used from pure Python code.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Use Horovod to train a distributed neural network using Parquet files + Petastorm
# MAGIC  
# MAGIC 
# MAGIC **Required Libraries**:
# MAGIC * `petastorm==0.7.5` via PyPI

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load data

# COMMAND ----------

from sklearn.datasets.california_housing import fetch_california_housing
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import numpy as np
import pandas as pd
np.random.seed(0)

cal_housing = fetch_california_housing()

# split 80/20 train-test
X_train, X_test, y_train, y_test = train_test_split(cal_housing.data,
                                                        cal_housing.target,
                                                        test_size=0.2,
                                                        random_state=1)

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark DataFrame
# MAGIC 
# MAGIC Let's concatenate our features and label, then create a Spark DataFrame from our Pandas DataFrame.

# COMMAND ----------

data = pd.concat([pd.DataFrame(X_train, columns=cal_housing.feature_names), pd.DataFrame(y_train, columns=["label"])], axis=1)
trainDF = spark.createDataFrame(data)
display(trainDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dense Vectors for Features

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

vecAssembler = VectorAssembler(inputCols=cal_housing.feature_names, outputCol="features")
vecTrainDF = vecAssembler.transform(trainDF).select("features", "label")
display(vecTrainDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Array
# MAGIC 
# MAGIC Petastorm requires an Array as input, not a Vector. Let's register a UDF in Scala and invoke it from Python for optimal performance.

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.linalg.Vector
# MAGIC val toArray = udf { v: Vector => v.toArray }
# MAGIC spark.udf.register("toArray", toArray)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Data
# MAGIC 
# MAGIC Let's write our DataFrame out as a parquet files to DBFS.

# COMMAND ----------

file_path = userhome + "/deep-learning/petastorm.parquet"
vecTrainDF.selectExpr("toArray(features) AS features", "label").repartition(8).write.mode("overwrite").parquet(file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove committed/started metadata
# MAGIC 
# MAGIC Petastorm + Horovod do not work if you leave the committed/started metadata files in our Parquet folder. We will need to remove them.

# COMMAND ----------

[dbutils.fs.rm(i.path) for i in dbutils.fs.ls(file_path) if ("_committed_" in i.name) | ("_started_" in i.name)]

display(dbutils.fs.ls(file_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Model

# COMMAND ----------

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import models, layers
tf.set_random_seed(42)

def build_model():
  model = models.Sequential()
  model.add(layers.Dense(20, input_dim=8, activation='relu'))
  model.add(layers.Dense(20, activation='relu'))
  model.add(layers.Dense(1, activation='linear'))
  return model

# COMMAND ----------

# MAGIC %md
# MAGIC ## Single Node
# MAGIC 
# MAGIC Define shape of the input tensor and output tensor and fit the model (on the driver). We need to use Petastorm's [make_batch_reader](https://petastorm.readthedocs.io/en/latest/api.html#petastorm.reader.make_batch_reader) to create an instance of Reader for reading batches out of a non-Petastorm Parquet store.

# COMMAND ----------

from petastorm import make_batch_reader
from petastorm.tf_utils import make_petastorm_dataset

abs_file_path = file_path.replace("dbfs:/", "/dbfs/")

with make_batch_reader("file://" + abs_file_path, num_epochs=100) as reader:
  dataset = make_petastorm_dataset(reader).map(lambda x: (tf.reshape(x.features, [-1,8]), tf.reshape(x.label, [-1,1])))
  model = build_model()
  optimizer = keras.optimizers.Adam(lr=0.001)
  model.compile(optimizer=optimizer,
                loss='mse',
                metrics=['mse'])
  model.fit(dataset, steps_per_epoch=10, epochs=10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Horovod
# MAGIC 
# MAGIC Let's do the same thing, but let's add in Horovod for distributed model training.

# COMMAND ----------

import horovod.tensorflow.keras as hvd

def run_training_horovod():
  # Horovod: initialize Horovod.
  hvd.init()
  with make_batch_reader("file://" + abs_file_path, num_epochs=100, cur_shard=hvd.rank(), shard_count= hvd.size()) as reader:
    dataset = make_petastorm_dataset(reader).map(lambda x: (tf.reshape(x.features, [-1,8]), tf.reshape(x.label, [-1,1])))
    model = build_model()
    optimizer = keras.optimizers.Adam(lr=0.001*hvd.size())
    optimizer = hvd.DistributedOptimizer(optimizer)
    model.compile(optimizer=optimizer,
                  loss='mse',
                  metrics=['mse'])
    history = model.fit(dataset, steps_per_epoch=10, epochs=10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train on driver

# COMMAND ----------

from sparkdl import HorovodRunner
hr = HorovodRunner(np=-1)
hr.run(run_training_horovod)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Better Horovod

# COMMAND ----------

import horovod.tensorflow.keras as hvd

dbutils.fs.rm(f"dbfs:/ml/{ml_username}/petastorm_checkpoint_weights.ckpt", True)
def run_training_horovod():
  # Horovod: initialize Horovod.
  hvd.init()
  with make_batch_reader("file://" + abs_file_path, num_epochs=100, cur_shard=hvd.rank(), shard_count= hvd.size()) as reader:
    dataset = make_petastorm_dataset(reader).map(lambda x: (tf.reshape(x.features, [-1,8]), tf.reshape(x.label, [-1,1])))
    model = build_model()
    optimizer = tf.keras.optimizers.Adam(lr=0.001*hvd.size())
    optimizer = hvd.DistributedOptimizer(optimizer)
    model.compile(optimizer=optimizer,
                  loss='mse',
                  metrics=['mse'])
    checkpoint_dir = f"/dbfs/ml/{ml_username}/petastorm_checkpoint_weights.ckpt"
    callbacks = [
    hvd.callbacks.BroadcastGlobalVariablesCallback(0),
    hvd.callbacks.MetricAverageCallback(),
    hvd.callbacks.LearningRateWarmupCallback(warmup_epochs=5, verbose=1),
    tf.keras.callbacks.ReduceLROnPlateau(monitor="loss", patience=10, verbose=1)
    ]

    if hvd.rank() == 0:
      callbacks.append(tf.keras.callbacks.ModelCheckpoint(checkpoint_dir, save_weights_only=True))
  
    history = model.fit(dataset, steps_per_epoch=10, epochs=10, callbacks = callbacks)

# COMMAND ----------

import horovod.tensorflow.keras as hvd
from sparkdl import HorovodRunner
hr = HorovodRunner(np=-1)
hr.run(run_training_horovod)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run on all workers

# COMMAND ----------

from sparkdl import HorovodRunner
hr = HorovodRunner(np=0)
hr.run(run_training_horovod)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>