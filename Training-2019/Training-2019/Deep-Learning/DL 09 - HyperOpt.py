# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # HyperOpt
# MAGIC 
# MAGIC The [HyperOpt library](https://github.com/hyperopt/hyperopt) allows for parallel hyperparameter tuning using either random search or Tree of Parzen Estimators (TPE). With MLflow, we can record the hyperparameters and corresponding metrics for each hyperparameter combination.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Use HyperOpt to train and optimize a feed-forward neural net
# MAGIC 
# MAGIC We will use the California Housing Dataset.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

from sklearn.datasets.california_housing import fetch_california_housing
from sklearn.model_selection import train_test_split
import numpy as np
np.random.seed(0)

cal_housing = fetch_california_housing()

# split 80/20 train-test
X_train, X_test, y_train, y_test = train_test_split(cal_housing.data,
                                                        cal_housing.target,
                                                        test_size=0.2,
                                                        random_state=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Standardization
# MAGIC Let's do feature-wise standardization.

# COMMAND ----------

from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Keras Model
# MAGIC 
# MAGIC We will define our NN in Keras and use the hyperparameters given by HyperOpt.

# COMMAND ----------

import tensorflow as tf
from tensorflow.keras.layers import Dense
from tensorflow.keras.models import Sequential
import mlflow
import mlflow.keras

def create_model(hpo):
  model = Sequential()
  model.add(Dense(int(hpo['dense_l1']), input_dim=8, activation='relu'))
  model.add(Dense(int(hpo['dense_l2']), activation='relu'))
  model.add(Dense(1, activation='linear'))
  return model

# COMMAND ----------

from hyperopt import fmin, hp, tpe, STATUS_OK, SparkTrials
def runNN(hpo):
  model = create_model(hpo)

  # Select Optimizer
  optimizer_call = getattr(tf.keras.optimizers, hpo["optimizer"])
  optimizer = optimizer_call(hpo["learning_rate"])

  # Compile model
  model.compile(loss="mse",
              optimizer=optimizer,
              metrics=["mse"])

  history = model.fit(X_train, y_train, validation_split=.2, epochs=10, verbose=2)

  # Evaluate our model
  score = model.evaluate(X_test, y_test, verbose=0)

  obj_metric = score[0]  
  return {"loss": obj_metric, "status": STATUS_OK}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup hyperparameter space and training
# MAGIC 
# MAGIC We need to create a search space for HyperOpt and set up SparkTrials to allow HyperOpt to run in parallel using Spark worker nodes. We can also start a MLflow run to automatically track the results of HyperOpt's tuning trials.

# COMMAND ----------

mlflow.set_experiment(f"/Users/{username}/tr-dl-mlflow")

batch_size = 64
single_node_epochs = 1

space = {'dense_l1': hp.quniform('dense_l1', 10, 30, 1),
         'dense_l2': hp.quniform('dense_l2', 10, 30, 1),
         'learning_rate': hp.loguniform('learning_rate', -5, 1),
         'optimizer': hp.choice('optimizer', ['Adadelta', 'Adam', 'RMSprop']),
        }

spark_trials = SparkTrials(parallelism=8)
with mlflow.start_run():
  best=fmin(runNN, space, algo=tpe.suggest, max_evals=30, trials = spark_trials)
best

# COMMAND ----------

# MAGIC %md
# MAGIC ### Retrieve Best Model

# COMMAND ----------

spark_trials.results[np.argmin([r["loss"] for r in spark_trials.results])]

# COMMAND ----------

# MAGIC %md
# MAGIC To view the MLflow experiment associated with the notebook, click the Runs icon in the notebook context bar on the upper right. There, you can view all runs. You can also bring up the full MLflow UI by clicking the button on the upper right that reads View Experiment UI when you hover over it.
# MAGIC 
# MAGIC To understand the effect of tuning a hyperparameter:
# MAGIC 
# MAGIC 0. Select the resulting runs and click Compare.
# MAGIC 0. In the Scatter Plot, select a hyperparameter for the X-axis and loss for the Y-axis.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>