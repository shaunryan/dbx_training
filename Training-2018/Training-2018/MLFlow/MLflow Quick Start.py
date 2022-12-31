# Databricks notebook source
# MAGIC %md ## MLflow Quick Start: Tracking
# MAGIC This is a notebook based on the MLflow quick start example.  This quick start:
# MAGIC * Creates an experiment and starts an MLflow run
# MAGIC * Logs parameters, metrics, and a file to the run
# MAGIC * Views the experiment, run, and notebook revision used in the run

# COMMAND ----------

# MAGIC %md 
# MAGIC **Note:** This notebook uses a Databricks hosted MLflow tracking server. If you would like to preview the Databricks MLflow tracking server, contact your Databricks sales representative to request access.

# COMMAND ----------

# MAGIC %md ### Set up and attach notebook to cluster

# COMMAND ----------

# MAGIC %md 
# MAGIC 1. Use or create a cluster with:
# MAGIC   * **Databricks Runtime Version:** Databricks Runtime 5.0 or above 
# MAGIC   * **Python Version:** Python 3
# MAGIC 1. Install MLflow library.
# MAGIC    1. Create library with Source **PyPI** and enter `mlflow`. See [PyPI package](https://docs.databricks.com/user-guide/libraries.html#pypi-libraries).
# MAGIC    1. Install the library into the cluster.
# MAGIC 1. Attach this notebook to the cluster.

# COMMAND ----------

# MAGIC %md ### Import MLflow and create experiment

# COMMAND ----------

# Import libraries

import os
import mlflow

# Set the experiment name to an experiment in the shared experiments folder

mlflow.set_experiment("/Shared/experiments/Quick Start")

# COMMAND ----------

# MAGIC %md ### Use the MLflow Tracking API
# MAGIC 
# MAGIC Use the [MLflow Tracking API](https://www.mlflow.org/docs/latest/python_api/index.html) to start a run and log parameters, metrics, and artifacts (files) from your data science code. 

# COMMAND ----------

# Start an MLflow run

with mlflow.start_run():
  # Log a parameter (key-value pair)
  mlflow.log_param("param1", 5)

  # Log a metric; metrics can be updated throughout the run
  mlflow.log_metric("foo", 1)
  mlflow.log_metric("foo", 2)
  mlflow.log_metric("foo", 3)

  # Log an artifact (output file)
  with open("output.txt", "w") as f:
      f.write("Hello world!")
  mlflow.log_artifact("output.txt")

# COMMAND ----------

# MAGIC %md  ## View the experiment and the run
# MAGIC 
# MAGIC 1. Open the experiment `/Shared/experiments/Quick Start` in the workspace: <img src="https://docs.databricks.com/_static/images/mlflow/quick-start-exp.png"/>
# MAGIC 1. Click a date to view a run: <img src="https://docs.databricks.com/_static/images/mlflow/quick-start-run.png"/>
# MAGIC 1. Click a source to view the notebook revision used in the run: <img src="https://docs.databricks.com/_static/images/mlflow/quick-start-notebook-rev.gif"/>