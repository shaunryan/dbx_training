# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab: Packaging a Project
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lab you:<br>
# MAGIC - Define an MLProject file
# MAGIC - Define a Conda environment
# MAGIC - Define your machine learning script
# MAGIC - Execute your solution as a run

# COMMAND ----------

# MAGIC %run "./../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining the MLproject file
# MAGIC 
# MAGIC Write an MLproject file called `MLproject` to the path defined for you below.

# COMMAND ----------

path = userhome + "/ml-production/03-lab/"

dbutils.fs.rm(path, True) # Clears the directory if it already exists
dbutils.fs.mkdirs(path)

print("Created directory `{}` to house the project files.".format(path))

# COMMAND ----------

# MAGIC %md
# MAGIC The file should consist of the following aspects:<br><br>
# MAGIC 
# MAGIC 0. The name should be `Lab-03`
# MAGIC 0. It should use the environment `conda.yaml`
# MAGIC 0. It should take the following parameters:
# MAGIC    - `data_path`: a string with a default of `/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv`
# MAGIC    - `bootstrap`: a boolean with a default of `True`
# MAGIC    - `min_impurity_decrease`: a float with a default of `0.`
# MAGIC 0. The command that uses the parameters listed above

# COMMAND ----------

#  TODO
dbutils.fs.put(path + "MLproject", 
'''

  FILL_IN

'''.strip())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining the Environment
# MAGIC 
# MAGIC Define the conda environment.  It should include the following libraries:<br><br>
# MAGIC 
# MAGIC   - `cloudpickle=0.5.3`
# MAGIC   - `numpy=1.14.3`
# MAGIC   - `pandas=0.23.0`
# MAGIC   - `scikit-learn=0.19.1`
# MAGIC   - `pip:`
# MAGIC     - `mlflow==1.0.0`

# COMMAND ----------

#  TODO
dbutils.fs.put(path + "conda.yaml", 
'''

  FILL_IN

'''.strip())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining the Machine Learning Script
# MAGIC 
# MAGIC Based on the script from Lesson 3, create a Random Forest model that uses the parameters `data_path`, `bootstrap`, and `min_impurity_decrease`.

# COMMAND ----------

#  TODO
dbutils.fs.put(path + "train.py", 
'''

  FILL_IN
  
'''.strip())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executing your Solution
# MAGIC 
# MAGIC First make sure that the three necessary files are where they need to be.

# COMMAND ----------

dbutils.fs.ls(path)

# COMMAND ----------

# MAGIC %md
# MAGIC Execute your solution with the following code.

# COMMAND ----------

import mlflow

mlflow.projects.run(uri=path.replace("dbfs:","/dbfs"),
  parameters={
    "data_path": "/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv",
    "bootstrap": False,
    "min_impurity_decrease": .1
})

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Next Lesson<br>
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See the solutions folder for an example solution to this lab.

# COMMAND ----------

# MAGIC %md
# MAGIC ### [Start the next lesson, Multistep Workflows.]($../04-Multistep-Workflows )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>