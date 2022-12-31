# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # MLflow Lab
# MAGIC 
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Add MLflow to your experiments from the Boston Housing Dataset!
# MAGIC  - Create a LambdaCallback
# MAGIC  - Create a UDF to apply your Keras model to a Spark DataFrame
# MAGIC  
# MAGIC **Bonus:**
# MAGIC * Modify your model (and track the parameters) to get the lowest MSE!

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

# Wait for the MLflow module to attactch to our cluster
# Utility method defined in Classroom-Setup
waitForMLflow()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load & Prepare Data

# COMMAND ----------

from sklearn.model_selection import train_test_split
from sklearn.datasets import load_boston
import numpy as np
np.random.seed(0)
from sklearn.preprocessing import StandardScaler

boston_housing = load_boston()

# split 80/20 train-test
X_train, X_test, y_train, y_test = train_test_split(boston_housing.data,
                                                        boston_housing.target,
                                                        test_size=0.2,
                                                        random_state=1)
# Scale features
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# Train-Validation Split
X_train_split, X_val, y_train_split, y_val = train_test_split(X_train,
                                                              y_train,
                                                              test_size=0.25,
                                                              random_state=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build_model
# MAGIC Create a `build_model()` function. Because Keras models are stateful, we want to get a fresh model every time we are trying out a new experiment.

# COMMAND ----------

import tensorflow as tf
tf.set_random_seed(42) # For reproducibility
from keras.models import Sequential
from keras.layers import Dense

def build_model():
  return Sequential([Dense(50, input_dim=13, activation='relu'),
                    Dense(20, activation='relu'),
                    Dense(1, activation='linear')])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start Using MLflow

# COMMAND ----------

import mlflow
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient

experiment_name = f"/Users/{username}/tr-dl-mlflow"
mlflow.set_experiment(experiment_name) # creates an experiment if it doesn't exist
mlflow_client = mlflow.tracking.MlflowClient()
experiment_id = mlflow_client.get_experiment_by_name(experiment_name).experiment_id

print(f"The experiment can be found at the path {experiment_name} and has an experiment_id of {experiment_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lambda Callback
# MAGIC 
# MAGIC Instead of logging all of the attributes of the history object once it has been trained (as in the previous notebook), let's create a [LambdaCallback](https://keras.io/callbacks/#lambdacallback) which can log your loss using MLflow at the end of each epoch!
# MAGIC 
# MAGIC **NOTE**: `on_epoch_end` expects two positional arguments: epoch and logs.

# COMMAND ----------

# TODO
from tensorflow.keras.callbacks import ModelCheckpoint, EarlyStopping, LambdaCallback

filepath = '/dbfs/user/' + username + '/deep-learning/keras_mlflow.h5'
checkpointer = ModelCheckpoint(filepath=filepath, verbose=1, save_best_only=True)
earlyStopping = EarlyStopping(monitor='val_loss', min_delta=0.0001, patience=2, mode='auto')

mlflowCallback = LambdaCallback(on_epoch_end = <FILL_IN>)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Track Experiments!
# MAGIC 
# MAGIC Now let's use MLflow to track our experiments. Try changing some of these hyperparameters around. 
# MAGIC * What do you find works best? 
# MAGIC * Log your parameters!

# COMMAND ----------

# MAGIC %md
# MAGIC Helper method to plot our validation vs training loss using matplotlib.

# COMMAND ----------

import matplotlib.pyplot as plt

def viewModelLoss(history):
  plt.clf()
  plt.semilogy(history.history['loss'], label='train_loss')
  plt.title('model loss')
  plt.ylabel('loss')
  plt.xlabel('epoch')
  plt.semilogy(history.history['val_loss'], label='val_loss')
  plt.legend()
  return plt

# COMMAND ----------

# TODO
from mlflow.keras import log_model

def run_mlflow(optimizer="adam", loss="mse", epochs=30, batch_size=32):
  with mlflow.start_run() as run:
      model = build_model()
      model.compile(optimizer=optimizer, loss=loss, metrics=["mae", "mse"])
      mlflow.log_param(<FILL_IN>)
      mlflow.log_param(<FILL_IN>)
      mlflow.log_param(<FILL_IN>)
      mlflow.log_param(<FILL_IN>)

      history = model.fit(X_train_split, y_train_split, validation_data=(X_val, y_val), 
                          epochs=epochs, batch_size=batch_size, callbacks=[checkpointer, earlyStopping, mlflowCallback], verbose=2)

      for i, layer in enumerate(model.layers):
        mlflow.log_param(f"hidden_layer_{i}_units", layer.output_shape)

      log_model(model, "keras_model")

      fig = viewModelLoss(history)
      fig.savefig("train-validation-loss.png")
      mlflow.log_artifact("train-validation-loss.png")
      display(plt.show())

run_mlflow()

# COMMAND ----------

# MAGIC %md
# MAGIC ## User Defined Function
# MAGIC 
# MAGIC Let's now register our Keras model as a Spark UDF to apply to rows in parallel.

# COMMAND ----------

# TODO
import pandas as pd
from mlflow.tracking import MlflowClient

predict = mlflow.pyfunc.spark_udf(<FILL_IN>)

X_test_DF = spark.createDataFrame(pd.concat([pd.DataFrame(X_test, columns=boston_housing.feature_names), 
                                             pd.DataFrame(y_test, columns=["label"])], axis=1))

display(X_test_DF.withColumn("prediction", predict(*boston_housing.feature_names)))


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>