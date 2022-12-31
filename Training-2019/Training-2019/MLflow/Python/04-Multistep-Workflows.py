# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Multistep Workflows
# MAGIC 
# MAGIC Machine learning projects quickly become complex when additional steps to the training process are added.  This lesson examines managing the complexity of multistep machine learning projects using multistep workflows.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Introduce multistep workflows
# MAGIC  - Execute a single, parameterized notebook as a step in a pipeline
# MAGIC  - Execute a multistep workflow

# COMMAND ----------

# MAGIC %md
# MAGIC ### Managing the Complexity
# MAGIC 
# MAGIC The concept of pipelines...<br><br>
# MAGIC 
# MAGIC  - Available in Scikit-Learn and Spark ML
# MAGIC  - Bundle multiple stages of a pipeline together in a single object
# MAGIC  - Works well as first step in handling complexity
# MAGIC  
# MAGIC But they're limited...<br><br>
# MAGIC 
# MAGIC  - Don't scale to support arbitrary code
# MAGIC  - Work only with built-in functions
# MAGIC  - Do not allow for monitoring of each stage in the process
# MAGIC 
# MAGIC **Multistep pipelines scales MLflow projects by allowing each stage to be its own project.**

# COMMAND ----------

# MAGIC %md
# MAGIC Multistep workflows...<br><br>
# MAGIC 
# MAGIC  - Allow for more robust pipelining with more complex stages
# MAGIC  - Use a flexible API
# MAGIC  - Work well for team collaboration: different people work on different stages
# MAGIC 
# MAGIC They work because **MLflow runs can recursively call other runs**  
# MAGIC 
# MAGIC This means that steps in a machine learning pipeline can be isolated.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Some details...<br><br>
# MAGIC 
# MAGIC  - There are a few options for passing artifacts between runs
# MAGIC  - They can be saved to a common location or they can be logged as an artifact within MLflow
# MAGIC  - This also allows for quicker iteration since runs can be run individually
# MAGIC  - You only need to rerun a given step if the last step's results have been cached
# MAGIC 
# MAGIC Below is an example of a multistep workflow with four distinct steps.  <a href="https://github.com/mlflow/mlflow/tree/master/examples/multistep_workflow" target="_blank">You can see the full repository here.</a>
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlproject-architecture0.png" style="height: 250px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Multistep Architecture
# MAGIC 
# MAGIC Now that we can package projects and run them in their environment, let's look at how  we can make workflows consisting of multiple steps.    There are three general architectures to consider:<br><br>
# MAGIC 
# MAGIC 1. One driver project calls other entry points in that same project
# MAGIC 2. One driver project calls other projects 
# MAGIC 3. One project calls another project as its final step
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlproject-architecture1.png" style="height: 250px; margin: 20px"/></div>
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlproject-architecture2.png" style="height: 250px; margin: 20px"/></div>
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlproject-architecture3.png" style="height: 250px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC In the last lesson, we used a Python file as a project that we ran.  In this lesson, we'll use Databricks notebooks instead.  These are all under the `Multistep` folder in your directory of notebooks.  There are two things to note about notebooks:<br><br>
# MAGIC 
# MAGIC 0. Notebooks can be run using `dbutils.notebook.run("<path>", "<timeout>")`
# MAGIC 0. Notebooks can be parameterized using widgets

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following code to see how this works.

# COMMAND ----------

step1 = dbutils.notebook.run("./Multistep/Step-1-Read-Data", 60, 
  {"data_input_path": "/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv"})

print(step1)

# COMMAND ----------

# MAGIC %md
# MAGIC This is the first step of our multistep workflow.  From the above, you can see that we ran the notebook `Step-1-Read-Data` in the `Multistep` directory.  We had a timeout of 60 seconds so if the code doesn't complete in that time, it would have failed.  Finally, we have the input path for our data.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC [Take a look at the underlying notebook.]($./Multistep/Step-1-Read-Data)  Note the following is accomplished in four cells:<br><br>
# MAGIC 
# MAGIC 0. Widgets are declared.  If you run these cells in the notebook itself, you'll see widgets appear at the top of the screen.  Widgets allow for the customization of notebooks without editing the code itself. They also allow for passing parameters into notebooks. 
# MAGIC 0. Widgets are read.  This allows you to get the value of the widget and use it in your code.
# MAGIC 0. An MLflow run logs the data from `data_input_path` as an artifact
# MAGIC 0. The notebook exits and reports back information to the parent notebook (in our case, that's this notebook).
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Check out <a href="https://docs.azuredatabricks.net/user-guide/notebooks/widgets.html#id1" target="_blank">the Databricks documentation on widgets for additional information </a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC We can get data back from the executed notebook by parsing the exit string.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> For debugging, you can either write your code in a notebook and then execute it as a run or you can click on `Notebook job #XXX` that appears after using `dbutils.notebook.run` to see where any problems might be.

# COMMAND ----------

import json

data_output_path = json.loads(step1).get("data_output_path")

print(data_output_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multistep Workflow
# MAGIC 
# MAGIC Now that we've created a single executable notebook with input parameters and output data, we can create more complex workflows.  [Take a look at the second step in our workflow.]($./Multistep/Step-2-Train)  This notebook takes the data logged as an artifact and trains a model using specific hyperparameters.  The trained model is also logged.

# COMMAND ----------

step2 = dbutils.notebook.run("./Multistep/Step-2-Train", 60, 
  {"artifact_URI": data_output_path,
   "n_estimators": 10,
   "max_depth": 20,
   "max_features": "auto"})

print(step2)

# COMMAND ----------

# MAGIC %md
# MAGIC Get the model output path from the result.

# COMMAND ----------

model_output_path = json.loads(step2).get("model_output_path")

print(model_output_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Now [take a look at the final step in our workflow.]($./Multistep/Step-3-Predict)  This step takes the saved model and creates predictions from it.  It logs those predictions as an artifact.

# COMMAND ----------

step3 = dbutils.notebook.run("./Multistep/Step-3-Predict", 60, 
  {"model_path": model_output_path,
   "data_path": data_output_path})

print(step3)

# COMMAND ----------

# MAGIC %md
# MAGIC Parse the notebook output to see the predictions

# COMMAND ----------

predictions_output_path = json.loads(step3).get("predictions_output_path")

print(dbutils.fs.head(dbutils.fs.ls(predictions_output_path)[0].path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Lab
# MAGIC 
# MAGIC 
# MAGIC ### [Click here to start the lab for this lesson.]($./Labs/04-Lab)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC 
# MAGIC **Question:** What are the benefits of pipelining?  
# MAGIC **Answer:** The biggest benefit to pipelining is managing the complexity of the machine learning process.  With loading, ETL, featurization, training, and prediction stages--to name a few--the complexity of the process quickly grows.  One additional benefit is collaboration since different individuals can work on different stages in the pipeline.
# MAGIC 
# MAGIC **Question:** How can I manage a pipeline using MLflow?  
# MAGIC **Answer:** Multi-step workflows chain together multiple MLflow jobs.  Runs can recursively call other runs.  This allows each stage to be its own MLflow project with its own environment, inputs, and outputs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find out more information on the future of Multistep Workflows?  
# MAGIC **A:** Check out Aaron Davidson's demo <a href="https://databricks.com/session/accelerating-the-machine-learning-lifecycle-with-mlflow-1-0" target="_blank">at Spark Summit 2019 for the direction of the project.</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>