# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # MLflow
# MAGIC 
# MAGIC ## Machine Learning Development is Complex 
# MAGIC 
# MAGIC Machine learning development takes place in multiple different phases:
# MAGIC 
# MAGIC 1. Data prepartion
# MAGIC 2. Model training 
# MAGIC 3. Model deployment
# MAGIC 4. Using results to influence the cycle
# MAGIC 
# MAGIC ![](https://www.evernote.com/l/AAF6kKLScy5Jr5Jae2v2kBnJ4Y-8HLuf9B8B/image.png)

# COMMAND ----------

# MAGIC %run "../../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC #### This work is often done across multiple teams.
# MAGIC 
# MAGIC Smaller organizations may have a single team dedicated to their data science lifecycle. 
# MAGIC 
# MAGIC Larger organizations will no doubt have this work split across multiple teams.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### This work is often done using multiple different technologies.
# MAGIC 
# MAGIC In addition to being split across multiple teams, this work also uses tools from a wide variety of sources and vendors. 
# MAGIC 
# MAGIC ![](https://www.evernote.com/l/AAGdLvlj9T5Lt6WsHehG0OjRL-zK3V2IlAAB/image.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Introducing MLflow
# MAGIC 
# MAGIC Open machine learning platform
# MAGIC 
# MAGIC - works with any ML library and language
# MAGIC - runs the same way anywhere
# MAGIC - designed to be useful for 1 or 100,000 person orgs

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install MLflow on Your Databricks Cluster
# MAGIC 
# MAGIC 1. Ensure you are using or [create a cluster](https://docs.azuredatabricks.net/user-guide/clusters/create.html#cluster-create) specifying 
# MAGIC   * **Databricks Runtime Version:** Databricks Runtime 4.1 (ML)
# MAGIC   * **Python Version:** Python 3
# MAGIC 2. Add `mlflow` as a PyPi library in Databricks, and install it on your cluster
# MAGIC   * Follow [Upload a Python PyPI package or Python Egg](https://docs.azuredatabricks.net/user-guide/libraries.html#upload-a-python-pypi-package-or-python-egg) to create a library
# MAGIC   * Choose **PyPi** and enter `mlflow==0.5.0` (this notebook was tested with `mlflow` version 0.5.0)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### MLflow Components
# MAGIC 
# MAGIC MLflow consists of three core components.
# MAGIC 
# MAGIC ![](https://www.evernote.com/l/AAHrjn2cEzpLobBWT0GTRIGl4KKrfOgfJnsB/image.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MLflow Tracking Server
# MAGIC 
# MAGIC With an MLflow Tracking Server, it is possible to track the team's experiments, recording code, datam configuration and results. These are then presented in an easily navigable UI for transparency into experiments being run.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ![](https://www.evernote.com/l/AAHxwfGhBjxIELLF26PDGd6Sl3cixtb8j-0B/image.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set up a Remote MLflow Tracking Server (already done here)
# MAGIC 
# MAGIC To run a long-lived, shared MLflow tracking server, we'll launch a Linux VM instance to run the [MLflow Tracking server](https://mlflow.org/docs/latest/tracking.html). To do this:
# MAGIC 
# MAGIC * Create a *Linux* instance
# MAGIC   * Open port 5000 for MLflow server; an example of how to do this via [How to open ports to a virtual machine with the Azure portal](https://docs.microsoft.com/en-us/azure/virtual-machines/windows/nsg-quickstart-portal). Opening up port 5000 to the Internet will allow anyone to access your server
# MAGIC   <!--, so it is recommended to only open up the port within an [Azure VPC](https://azure.microsoft.com/en-us/services/virtual-network/) that your Databricks clusters have access to. -->
# MAGIC   * **TODO** These are for Azure. Need instructions for both AWS and Azure.
# MAGIC * Install MLflow and Run the Tracking Server
# MAGIC   * For more information, refer to [MLflow > Running a Tracking Server](https://mlflow.org/docs/latest/tracking.html?highlight=server#running-a-tracking-server).
# MAGIC   * Using Conda
# MAGIC     * Install conda onto your Linux instance via [Conda > Installing on Linux](https://conda.io/docs/user-guide/install/linux.html)
# MAGIC     * Install `mlflow`
# MAGIC     
# MAGIC       `$ pip install mlflow`
# MAGIC       
# MAGIC     * Run the `server` command in MLflow passing it `--host 0.0.0.0`
# MAGIC       
# MAGIC       `$ mlflow server --host 0.0.0.0`
# MAGIC       
# MAGIC   * Using Docker
# MAGIC     * Build and compile the `Dockerfile` included with the MLflow repo
# MAGIC       
# MAGIC       `$ cd mlflow`  
# MAGIC       `$ docker build -t mlflow . # make sure to include the period`
# MAGIC       
# MAGIC     * Run the `server` command in MLflow passing it `--host 0.0.0.0`
# MAGIC       
# MAGIC       `$ docker run -it -p 5000:5000 mlflow mlflow server --host 0.0.0.0`
# MAGIC       
# MAGIC   * To test connectivity of your tracking server:
# MAGIC     * Get the hostname of your instance
# MAGIC     * Go to http://$TRACKING_SERVER$:5000; it should look similar to this [MLflow UI](https://databricks.com/wp-content/uploads/2018/06/mlflow-web-ui.png)
# MAGIC 
# MAGIC * **NOTE**: We can only save parameters and metrics to this remote tracking server that has been pre-configured for this class.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start Using MLflow Tracking in a Notebook
# MAGIC 
# MAGIC The first step is to import call `mlflow.set_tracking_uri` to point to your server:

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Set this variable to your MLflow server's DNS name
# MAGIC mlflow_server = '104.208.136.67'
# MAGIC 
# MAGIC # Tracking URI
# MAGIC mlflow_tracking_URI = 'http://' + mlflow_server + ':5000'
# MAGIC print ("MLflow Tracking URI: {}".format(mlflow_tracking_URI))
# MAGIC 
# MAGIC # Import MLflow and set the Tracking UI
# MAGIC import mlflow
# MAGIC mlflow.set_tracking_uri(mlflow_tracking_URI)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Decision Trees
# MAGIC ### Analyzing a bike sharing dataset
# MAGIC 
# MAGIC In ML 03 - SparkML Lab, we ran some experiments on the bike sharing dataset using tree-based models. Let's look at this approach to this dataset once more. 
# MAGIC 
# MAGIC ### Preprocess data
# MAGIC 
# MAGIC So what do we need to do to get our data ready for Machine Learning?
# MAGIC 
# MAGIC *Recall our goal*: We want to learn to predict the count of bike rentals (the `cnt` column).  We refer to the count as our target "label".
# MAGIC 
# MAGIC *Features*: What can we use as features to predict the `cnt` label?  All the columns except `cnt`, and a few exceptions:
# MAGIC * The `cnt` column we want to predict equals the sum of the `casual` + `registered` columns.  We will remove the `casual` and `registered` columns from the data to make sure we do not use them to predict `cnt`.  (*Warning: This is a danger in careless Machine Learning.  Make sure you do not "cheat" by using information you will not have when making predictions*)
# MAGIC * date column `dteday`: We could keep it, but it is well-represented by the other date-related columns `season`, `yr`, `mnth`, and `weekday`.  We will discard it.
# MAGIC * `holiday` and `weekday`: These features are highly correlated with the `workingday` column.
# MAGIC * row index column `instant`: This is a useless column to us.
# MAGIC 
# MAGIC ### Train/Test Split
# MAGIC 
# MAGIC Our final data preparation step will be to split our dataset into separate training and test sets.
# MAGIC 
# MAGIC Use `randomSplit` to split the data such that 70% of the data is reserved for training, and the remaining 30% for testing. Use the set `seed` for reproducability (i.e. if you re-run this notebook or compare with your neighbor, you will get the same results).
# MAGIC 
# MAGIC ### Train a Machine Learning Pipeline
# MAGIC 
# MAGIC Let's learn a ML model to predict the `cnt` of bike rentals given a single `features` column of feature vectors. 
# MAGIC 
# MAGIC We will put together a simple Pipeline with the following stages:
# MAGIC * `VectorAssembler`: Assemble the feature columns into a feature vector.
# MAGIC * `VectorIndexer`: Identify columns which should be treated as categorical.  This is done heuristically, identifying any column with a small number of distinct values as being categorical.  For us, this will be the `yr` (2 values), `season` (4 values), `holiday` (2 values), `workingday` (2 values), and `weathersit` (4 values).
# MAGIC * `DecisionTreeRegressor`: This will build a decision tree to learn how to predict rental counts from the feature vectors.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Code taken from previous lab
# MAGIC from pyspark.ml.feature import VectorAssembler, VectorIndexer
# MAGIC 
# MAGIC df = (spark
# MAGIC       .read
# MAGIC       .option("header", "true")
# MAGIC       .option("inferSchema", "true")
# MAGIC       .csv("/databricks-datasets/bikeSharing/data-001/hour.csv")
# MAGIC       .drop("instant", "dteday", "casual", "registered", "holiday", "weekday"))
# MAGIC 
# MAGIC df.cache()
# MAGIC 
# MAGIC trainDF, testDF = df.randomSplit([0.7, 0.3], seed=42)
# MAGIC 
# MAGIC featuresCols = df.columns[:-1] # Removes "cnt"
# MAGIC 
# MAGIC vectorAssembler = VectorAssembler(inputCols=featuresCols, outputCol="rawFeatures")
# MAGIC 
# MAGIC vectorIndexer = VectorIndexer(inputCol="rawFeatures", outputCol="features", maxCategories=4)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Experiment Definition
# MAGIC 
# MAGIC ### Predicting bike share usage
# MAGIC 
# MAGIC We will run five different experiments on this dataset with the following model specifications:
# MAGIC 
# MAGIC 1. A basic decision tree
# MAGIC 1. a basic random forest
# MAGIC 1. a grid searched random forest
# MAGIC 1. an elastic net with 0.5 l1 ratio and 1E2 regularization parameter
# MAGIC 1. an elastic net with 0.4 l1 ratio and 1E-3 regularization parameter
# MAGIC 
# MAGIC Each of these will be assessed with two different regression metrics:
# MAGIC 
# MAGIC 1. the root mean square error (rmse)
# MAGIC 1. the coefficient of determination (r2)
# MAGIC 
# MAGIC We'll use [RegressionEvaluator](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.evaluation.RegressionEvaluator) to assess the results. 
# MAGIC 
# MAGIC ### Logging Parameters and Metrics 
# MAGIC 
# MAGIC We use two `mlflow` API methods to log to the MLflow Server:
# MAGIC 
# MAGIC - [mlflow.log_param](https://mlflow.org/docs/latest/python_api/mlflow.html#mlflow.log_param)
# MAGIC - [mlflow.log_metric](https://mlflow.org/docs/latest/python_api/mlflow.html#mlflow.log_metric)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Experiment 1 - Basic Decision Tree

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml.regression import DecisionTreeRegressor
# MAGIC from pyspark.ml.evaluation import RegressionEvaluator
# MAGIC from pyspark.ml import Pipeline
# MAGIC 
# MAGIC with mlflow.start_run():
# MAGIC   
# MAGIC   # define model and record parameters
# MAGIC   dt = DecisionTreeRegressor().setLabelCol("cnt")
# MAGIC   mlflow.log_param("model", "dt")
# MAGIC   mlflow.log_param("experiment", "1")
# MAGIC   mlflow.log_param("depth", dt.getMaxDepth())
# MAGIC 
# MAGIC   # build and fit model pipeline
# MAGIC   pipeline = Pipeline().setStages([vectorAssembler, vectorIndexer, dt])
# MAGIC   pipelineModel = pipeline.fit(trainDF)
# MAGIC   predictionsDF = pipelineModel.transform(testDF)
# MAGIC   
# MAGIC   # evaluate fit model and record metrics
# MAGIC   evaluator = RegressionEvaluator().setLabelCol("cnt")
# MAGIC   r2 = evaluator.evaluate(predictionsDF, {evaluator.metricName: "r2"})
# MAGIC   rmse = evaluator.evaluate(predictionsDF, {evaluator.metricName: "rmse"})
# MAGIC   mlflow.log_metric("r2", r2)
# MAGIC   mlflow.log_metric("rmse", rmse)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Experiment 2 - Basic Random Forest

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml.regression import RandomForestRegressor
# MAGIC 
# MAGIC with mlflow.start_run(): 
# MAGIC   
# MAGIC   # define model and record parameters
# MAGIC   rf = RandomForestRegressor().setLabelCol("cnt").setSeed(27)
# MAGIC   mlflow.log_param("model", "rf")
# MAGIC   mlflow.log_param("depth", rf.getMaxDepth())
# MAGIC   mlflow.log_param("num_trees", rf.getNumTrees())
# MAGIC   mlflow.log_param("experiment", "2")
# MAGIC 
# MAGIC   # build and fit model pipeline
# MAGIC   pipeline = Pipeline().setStages([vectorAssembler, vectorIndexer, rf])
# MAGIC   pipelineModel = pipeline.fit(trainDF)
# MAGIC   predictionsDF = pipelineModel.transform(testDF)
# MAGIC   
# MAGIC   # evaluate fit model and record metrics
# MAGIC   evaluator = RegressionEvaluator().setLabelCol("cnt")
# MAGIC   r2 = evaluator.evaluate(predictionsDF, {evaluator.metricName: "r2"})
# MAGIC   rmse = evaluator.evaluate(predictionsDF, {evaluator.metricName: "rmse"})
# MAGIC   mlflow.log_metric("r2", r2)
# MAGIC   mlflow.log_metric("rmse", rmse)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Experiment 3 - Grid Searched Random Forest

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml.tuning import ParamGridBuilder
# MAGIC from pyspark.ml.tuning import CrossValidator
# MAGIC 
# MAGIC with mlflow.start_run(): 
# MAGIC   
# MAGIC   # define model and record parameters
# MAGIC   rf = RandomForestRegressor().setLabelCol("cnt").setSeed(27)
# MAGIC   paramGrid = (ParamGridBuilder()
# MAGIC           .addGrid(rf.maxDepth, [2, 5, 10])
# MAGIC           .addGrid(rf.numTrees, [10, 50])
# MAGIC           .build())
# MAGIC   mlflow.log_param("model", "rf")
# MAGIC   mlflow.log_param("experiment", "3")
# MAGIC 
# MAGIC   # build and fit model pipeline
# MAGIC   pipeline = Pipeline().setStages([vectorAssembler, vectorIndexer, rf])
# MAGIC   cv = (CrossValidator()
# MAGIC       .setEstimator(pipeline)
# MAGIC       .setEvaluator(evaluator)
# MAGIC       .setEstimatorParamMaps(paramGrid)
# MAGIC       .setNumFolds(3)
# MAGIC       .setSeed(27))
# MAGIC 
# MAGIC   cvModel = cv.fit(trainDF)
# MAGIC   bestRFmodel = cvModel.bestModel.stages[2]
# MAGIC   fitParamMap = bestRFmodel.extractParamMap()
# MAGIC   bestNumTrees = fitParamMap[bestRFmodel.getParam('numTrees')]
# MAGIC   bestMaxDepth = fitParamMap[bestRFmodel.getParam('maxDepth')]
# MAGIC   
# MAGIC   predictionsDF = cvModel.transform(testDF)
# MAGIC   mlflow.log_param("depth", bestNumTrees)
# MAGIC   mlflow.log_param("num_trees", bestMaxDepth)
# MAGIC   
# MAGIC   # evaluate fit model and record metrics
# MAGIC   evaluator = (RegressionEvaluator()
# MAGIC              .setLabelCol("cnt")
# MAGIC              .setPredictionCol("prediction"))
# MAGIC   r2 = evaluator.evaluate(predictionsDF, {evaluator.metricName: "r2"})
# MAGIC   rmse = evaluator.evaluate(predictionsDF, {evaluator.metricName: "rmse"})
# MAGIC   mlflow.log_metric("r2", r2)
# MAGIC   mlflow.log_metric("rmse", rmse)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Experiment 4 - Elastic Net 1

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml.regression import LinearRegression
# MAGIC 
# MAGIC 
# MAGIC with mlflow.start_run(): 
# MAGIC   
# MAGIC   # define model and record parameters
# MAGIC   lm = LinearRegression().setLabelCol("cnt")
# MAGIC   lm.setElasticNetParam(0.5)
# MAGIC   lm.setRegParam(1E2)
# MAGIC   lm.setStandardization(False)
# MAGIC 
# MAGIC   mlflow.log_param("model", "lm")
# MAGIC   mlflow.log_param("experiment", "4")
# MAGIC   mlflow.log_param("elastic net param", lm.getElasticNetParam())
# MAGIC   mlflow.log_param("regularization param", lm.getRegParam())
# MAGIC   mlflow.log_param("standardization", lm.getStandardization())
# MAGIC 
# MAGIC   # build and fit model pipeline
# MAGIC   pipeline = Pipeline().setStages([vectorAssembler, vectorIndexer, lm])
# MAGIC   pipelineModel = pipeline.fit(trainDF)
# MAGIC   predictionsDF = pipelineModel.transform(testDF)
# MAGIC   
# MAGIC   # evaluate fit model and record metrics
# MAGIC   evaluator = RegressionEvaluator().setLabelCol("cnt")
# MAGIC   r2 = evaluator.evaluate(predictionsDF, {evaluator.metricName: "r2"})
# MAGIC   rmse = evaluator.evaluate(predictionsDF, {evaluator.metricName: "rmse"})
# MAGIC   mlflow.log_metric("r2", r2)
# MAGIC   mlflow.log_metric("rmse", rmse)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Experiment 5 - Elastic Net 2

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml.regression import LinearRegression
# MAGIC 
# MAGIC with mlflow.start_run(): 
# MAGIC   
# MAGIC   # define model and record parameters
# MAGIC   lm = LinearRegression().setLabelCol("cnt")
# MAGIC   lm.setElasticNetParam(0.4)
# MAGIC   lm.setRegParam(1E-3)
# MAGIC   lm.setStandardization(True)
# MAGIC 
# MAGIC   mlflow.log_param("model", "lm")
# MAGIC   mlflow.log_param("experiment", "5")
# MAGIC   mlflow.log_param("elastic net param", lm.getElasticNetParam())
# MAGIC   mlflow.log_param("regularization param", lm.getRegParam())
# MAGIC   mlflow.log_param("standardization", lm.getStandardization())
# MAGIC 
# MAGIC   # build and fit model pipeline
# MAGIC   pipeline = Pipeline().setStages([vectorAssembler, vectorIndexer, lm])
# MAGIC   pipelineModel = pipeline.fit(trainDF)
# MAGIC   predictionsDF = pipelineModel.transform(testDF)
# MAGIC   
# MAGIC   # evaluate fit model and record metrics
# MAGIC   evaluator = RegressionEvaluator().setLabelCol("cnt")
# MAGIC   r2 = evaluator.evaluate(predictionsDF, {evaluator.metricName: "r2"})
# MAGIC   rmse = evaluator.evaluate(predictionsDF, {evaluator.metricName: "rmse"})
# MAGIC   mlflow.log_metric("r2", r2)
# MAGIC   mlflow.log_metric("rmse", rmse)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review the MLflow UI
# MAGIC Open the URL of your tracking server in a web browser. In case you forgot it, you can get it from `mlflow.get_tracking_uri()`:

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Identify the location of the runs
# MAGIC mlflow.tracking.get_tracking_uri()

# COMMAND ----------

# MAGIC %md
# MAGIC The MLflow UI should look something similar to the animated GIF below. Inside the UI, you can:
# MAGIC * View your experiments and runs
# MAGIC * Review the parameters and metrics on each run
# MAGIC * Click each run for a detailed view to see the the model, images, and other artifacts produced.
# MAGIC 
# MAGIC <img src="https://brookewenig.github.io/img/DL/mlflow-ui-azure.gif"/>

# COMMAND ----------

# MAGIC %md
# MAGIC # MLflow Projects
# MAGIC 
# MAGIC MLflow Projects are another way to leverage MLflow for sharing and consolidating work across a team.
# MAGIC 
# MAGIC At the core of MLflow Projects are a yaml-based specification that can be used to share projects.
# MAGIC 
# MAGIC For demonstration purposes, a repository containing a machine learning project has been created on Github:
# MAGIC 
# MAGIC https://github.com/databricks/mlflow-example-sklearn-elasticnet-wine 
# MAGIC 
# MAGIC This project contains the following files:
# MAGIC 
# MAGIC ```
# MAGIC .
# MAGIC ├── MLproject
# MAGIC ├── conda.yaml
# MAGIC ├── train.ipynb
# MAGIC ├── train.py
# MAGIC └── wine-quality.csv
# MAGIC ```
# MAGIC 
# MAGIC This work is being done on the data set: http://archive.ics.uci.edu/ml/datasets/Wine+Quality. The data set has been stored in the repository as the file `wine-quality.csv`. 
# MAGIC 
# MAGIC The notebook included here `train.ipynb` demonstrates a sample model development notebook. The work done in this notebook is then added to the file `train.py`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## `MLProject`
# MAGIC 
# MAGIC Finally, the file `MLproject` defines how a training run will be performed. This file specifies:
# MAGIC 
# MAGIC ```
# MAGIC name: tutorial
# MAGIC 
# MAGIC conda_env: conda.yaml
# MAGIC 
# MAGIC entry_points:
# MAGIC   main:
# MAGIC     parameters:
# MAGIC       alpha: float
# MAGIC       l1_ratio: {type: float, default: 0.1}
# MAGIC     command: "python train.py {alpha} {l1_ratio}"
# MAGIC ```
# MAGIC 
# MAGIC This specifies:
# MAGIC   
# MAGIC - the name of the experiment
# MAGIC - the environment in which the experiment should be run
# MAGIC    - described in `conda.yaml`
# MAGIC - the main process that should be executed to run the experiment 
# MAGIC    - `python train.py` with the parameters `alpha` and `l1_ratio`
# MAGIC    - note that a default value has been specified for `l1_ratio`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run an Experiment
# MAGIC 
# MAGIC Given these specifications, we can run an experiment by simply calling `mlflow.run()` and passing the `uri` referencing the repository and any desired parameters.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC mlflow.run(
# MAGIC   uri="https://github.com/databricks/mlflow-example-sklearn-elasticnet-wine",
# MAGIC   parameters={'alpha':1E3}
# MAGIC )

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC mlflow.run(
# MAGIC   uri="https://github.com/databricks/mlflow-example-sklearn-elasticnet-wine",
# MAGIC   parameters={'alpha':1E-1}
# MAGIC )

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from numpy import logspace 
# MAGIC 
# MAGIC for alpha in logspace(-3,3,7):
# MAGIC   mlflow.run(
# MAGIC     uri="https://github.com/databricks/mlflow-example-sklearn-elasticnet-wine",
# MAGIC     parameters={'alpha': alpha}
# MAGIC   )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>