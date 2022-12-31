// Databricks notebook source
// MAGIC %md ## MLflow Quick Start: Tracking
// MAGIC This is a notebook based on the MLflow quick start example.  This quick start:
// MAGIC * Creates an experiment and starts an MLflow run
// MAGIC * Logs parameters, metrics, and a file to the run
// MAGIC * Views the experiment, run, and notebook revision used in the run

// COMMAND ----------

// MAGIC %md ### Set up and attach notebook to cluster

// COMMAND ----------

// MAGIC %md 
// MAGIC 1. Use or create a cluster with:
// MAGIC   * **Databricks Runtime Version:** Databricks Runtime 5.0 or above 
// MAGIC 1. Install MLflow library.
// MAGIC    1. Create library with Source **PyPI** and enter `mlflow`. See [PyPI package](https://docs.databricks.com/user-guide/libraries.html#pypi-libraries).
// MAGIC    1. Create library with Source **Maven** and enter `org.mlflow:mlflow-client:0.8.2`.
// MAGIC    1. Install the libraries into the cluster.
// MAGIC 1. Attach this notebook to the cluster.

// COMMAND ----------

import org.mlflow.tracking.MlflowClient
import org.mlflow.api.proto.Service.RunStatus
import java.io.{File,PrintWriter}

// COMMAND ----------

// MAGIC %md ### Create MLflow client and experiment

// COMMAND ----------

val mlflowClient = new MlflowClient()

// COMMAND ----------

// MAGIC %md ### Use the MLflow Tracking API
// MAGIC 
// MAGIC Use the [MLflow Tracking API](https://www.mlflow.org/docs/latest/python_api/index.html) to start a run and log parameters, metrics, and artifacts (files) from your data science code. 

// COMMAND ----------

// MAGIC %md ### Create experiment and run and fetch run ID

// COMMAND ----------

val expId = mlflowClient.createExperiment("/Shared/experiments/Quick Start")
val runInfo = mlflowClient.createRun(expId)
val runId = runInfo.getRunUuid()

// COMMAND ----------

// MAGIC %md ### Log parameters, metrics, and file

// COMMAND ----------

// Log a parameter (key-value pair)
mlflowClient.logParam(runId, "param1", "5")

// Log a metric; metrics can be updated throughout the run
mlflowClient.logMetric(runId, "foo", 1.0);
mlflowClient.logMetric(runId, "foo", 2.0);
mlflowClient.logMetric(runId, "foo", 3.0);

new PrintWriter("/tmp/output.txt") { write("Hello, world!") ; close }
mlflowClient.logArtifact(runId, new File("/tmp/output.txt"))

// COMMAND ----------

// MAGIC %md ### Close the run

// COMMAND ----------

mlflowClient.setTerminated(runId, RunStatus.FINISHED, System.currentTimeMillis())

// COMMAND ----------

// MAGIC %md  ## View the experiment and the run
// MAGIC 
// MAGIC 1. Open the experiment `/Shared/experiments/Quick Start` in the workspace: <img src="https://docs.databricks.com/_static/images/mlflow/quick-start-exp.png"/>
// MAGIC 1. Click a date to view a run: <img src="https://docs.databricks.com/_static/images/mlflow/quick-start-run.png"/>
// MAGIC 1. Click a source to view the notebook revision used in the run: <img src="https://docs.databricks.com/_static/images/mlflow/quick-start-notebook-rev.gif"/>