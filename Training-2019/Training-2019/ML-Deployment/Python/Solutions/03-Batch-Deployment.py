# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Batch Deployment
# MAGIC 
# MAGIC Batch inference is the most common way of deploying machine learning models.  This lesson introduces various strategies for deploying models using batch including pure Python, Spark, and on the JVM.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Explore batch deployment options
# MAGIC  - Predict on a Pandas DataFrame and save the results
# MAGIC  - Predict on a Spark DataFrame and save the results
# MAGIC  - Create an Azure resource group and a Cosmos database
# MAGIC  - Write predictions to Cosmos DB
# MAGIC  - Compare other batch deployment options

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inference in Batch
# MAGIC 
# MAGIC Batch deployment represents the vast majority of use cases for deploying machine learning models.<br><br>
# MAGIC 
# MAGIC * This normally means running the predictions from a model and saving them somewhere for later use.
# MAGIC * For live serving, results are often saved to a database that will serve the saved prediction quickly.
# MAGIC * In other cases, such as populating emails, they can be stored in less performant data stores such as a blob store.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/batch-predictions.png" width=800px />
# MAGIC 
# MAGIC Writing the results of an inference can be optimized in a number of ways...<br><br>
# MAGIC 
# MAGIC * For large amounts of data, predictions and writes should be performed in parallel
# MAGIC * **The access pattern for the saved predictions should also be kept in mind in how the data is written**
# MAGIC   - For static files or data warehouses, partitioning speeds up data reads
# MAGIC   - For databases, indexing the database on the relevant query generally improves performance
# MAGIC   - In either case, the index is working similar to an index in a book: it allows you to skip ahead to the relevant content

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC There are a few other considerations to ensure the accuracy of a model...<br><br>
# MAGIC 
# MAGIC * First is to make sure that the model matches expectations
# MAGIC   - We'll cover this in further detail in the model drift section
# MAGIC * Second is to **retrain the model on the majority of your dataset**
# MAGIC   - Either use the entire dataset for training or around 95% of it
# MAGIC   - A train/test split is a good method in tuning hyperparameters and estimating how the model will perform on unseen data
# MAGIC   - Retraining the model on the majority of the dataset ensures that you have as much information as possible factored into the model

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inference in Pure Python
# MAGIC 
# MAGIC Inference in Python leverages the predict functionality of the machine learning package or MLflow wrapper.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Import the data.  **Do not perform a train/test split.**
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> It is common to skip the train/test split in training a final model.

# COMMAND ----------

import pandas as pd
from sklearn.model_selection import train_test_split

df = pd.read_csv("/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv")

X = df.drop(["price"], axis=1)
y = df["price"]

# COMMAND ----------

# MAGIC %md
# MAGIC Train a final model

# COMMAND ----------

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

rf = RandomForestRegressor(n_estimators=100, max_depth=5)
rf.fit(X, y)

predictions = X.copy()
predictions["prediction"] = rf.predict(X)

mse = mean_squared_error(y, predictions["prediction"]) # This is on the same data the model was trained

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Save the results and partition by zip code.  Note that zip code was indexed.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Should zip code be modeled as a continuous or categorical feature?

# COMMAND ----------

import os

path = userhome + "/ml-production/mlflow-model-training/batch-predictions/"

dbutils.fs.rm(path, True)

for i, partition in predictions.groupby(predictions["zipcode"]):
  dirpath = path.replace("dbfs:", "/dbfs") + str(i)
  print("Writing to {}".format(dirpath))
  os.makedirs(dirpath)

  partition.to_csv(dirpath + "/predictions.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Log the model and predictions.

# COMMAND ----------

import mlflow.sklearn
from sklearn.metrics import mean_squared_error

with mlflow.start_run(run_name="Final RF Model") as run:
  mlflow.sklearn.log_model(rf, "random-forest-model")
  mlflow.log_metric("Train data MSE", mse)

  mlflow.log_artifacts(path, "predictions")

  run_info = run.info

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Inference in Spark
# MAGIC 
# MAGIC Models trained in various machine learning libraries can be applied at scale using Spark.  To do this, use `mlflow.pyfunc.spark_udf` and pass in the `SparkSession`, name of the model, and run id.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Using UDF's in Spark means that supporting libraries must be installed on every node in the cluster.  In the case of `sklearn`, this is installed in Databricks clusters by default.  When using other libraries, install them using the UI in order to ensure that they will work as UDFs.

# COMMAND ----------

# MAGIC %md
# MAGIC Create a Spark DataFrame from the Pandas DataFrame.

# COMMAND ----------

XDF = spark.createDataFrame(X)

display(XDF)

# COMMAND ----------

# MAGIC %md
# MAGIC MLflow easily produces a Spark user defined function (UDF).  This bridges the gap between Python environments and applying models at scale using Spark.

# COMMAND ----------

predict = mlflow.pyfunc.spark_udf(spark, run.info.artifact_uri + "/random-forest-model")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Apply the model as a standard UDF using the column names as the input to the function.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Python has an internal limit to the maximum number of arguments you can pass to a function.  The maximum number of features in a model applied in this way is therefore 255.  This limit will be changed in Python 3.7.

# COMMAND ----------

predictionDF = XDF.withColumn("prediction", predict(*X.columns))

display(predictionDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Now write the results to DBFS as parquet.

# COMMAND ----------

parquet_path = userhome + "/ml-production/mlflow-model-training/batch-predictions.parquet"

predictionDF.write.mode("OVERWRITE").parquet(parquet_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the results.

# COMMAND ----------

display(dbutils.fs.ls(parquet_path))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Cosmos DB for Prediction Serving
# MAGIC 
# MAGIC Cosmos DB is Microsoft Azure's document database.  It is low latency and high availability.  One of the core benefits of this technology is that you can globally replicate data easily, always keeping data up to date across the globe.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> **Please be sure to delete any infrastructure you build after the course so you don't incur unexpected expenses.**
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> If you're executing these steps yourself, you will need access to the Azure Portal with the permissions to create resources.

# COMMAND ----------

# MAGIC %md
# MAGIC You will need to install the Azure connector from the Maven coordinates `com.microsoft.azure:azure-cosmosdb-spark_2.4.0_2.11:1.3.5`
# MAGIC 
# MAGIC <a href="https://files.training.databricks.com/static/step-by-step/installing-libraries-from-maven.html" target="_blank">See the instructions on how to install a library from Maven</a> if you're unfamiliar with the process.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Create a new resource group to contain all of the infrastructure built in this course.  It allows you to easily delete the infrastructure by deleting the resource group.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Some services we use in this course, such as Azure Kubernetes Service will create their own resource groups and they might need to be deleted separately
# MAGIC <br><br>
# MAGIC 
# MAGIC 1. Access the Azure Portal via the link in Azure Databricks.
# MAGIC <div><img src="https://files.training.databricks.com/images/airlift/AAHy9d889ERM6rJd2US1kRiqGCLiHzgmtFsB.png" width=800px/></div>
# MAGIC 2. Choose **Resource groups**.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/resourcegroup2.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 3. Choose **Add**.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/resourcegroup3.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 4. Choose the subscription you'd like to use.  If you're unclear on this, contact your administrator.  Name the resource group something like `ml-deployment` and select `West US` as your region.  Click **Review + create**.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/resourcegroup4.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 5. Confirm the information and click **Create**.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/resourcegroup5.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 6. Navigate to the new resource group you created and copy the **Subscription ID**.  You'll use this number and resource group many times in this course.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/resourcegroup6.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Create the Cosmos database and collection by performing the following steps: <br><br>
# MAGIC 
# MAGIC 1. Access the Azure Portal via the link in Azure Databricks.
# MAGIC <div><img src="https://files.training.databricks.com/images/airlift/AAHy9d889ERM6rJd2US1kRiqGCLiHzgmtFsB.png" width=800px/></div>
# MAGIC 2. Choose **Azure Cosmos DB**.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/cosmos1.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 3. Choose **Add**.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/cosmos2.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 4. Insert the information seen below.  Choose your own subscription and resource group.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/cosmos3.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 5. Create a new collection for your predictions using the information below.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/cosmos5-update.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 6. Copy the key and URI from the keys tab.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/cosmos4.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can run the following cell and the rest of the notebook as is for read only access to Cosmos DB.  This will access infrastructure that Databricks maintains.  If you want write access, you will have to follow the steps above and provide your own keys in [this notebook that defines your Azure credentials]($./Includes/Azure-Credentials ).
# MAGIC 
# MAGIC The notebook defines the following variables and was automatically run in our classroom setup script above:<br><br>
# MAGIC 
# MAGIC 1. `cosmos_uri`
# MAGIC 1. `cosmos_database`
# MAGIC 1. `cosmos_container`
# MAGIC 1. `cosmos_read_key`
# MAGIC 1. `cosmos_read_write_key` (you can replace this with your own keys)

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following to confirm the credentials are as expected.

# COMMAND ----------

print("Cosmos URI:\t\t" + cosmos_uri)
print("Cosmos Database:\t" + cosmos_database)
print("Cosmos Container:\t" + cosmos_container)
print("Cosmos Read Key:\t" + cosmos_read_key) # This is publicly sharable since it is read only

if cosmos_read_write_key:
  print("Read/Write Key:\tis defined")
else:
  print("Read/Write Key:\t\tis NOT defined")

# COMMAND ----------

# MAGIC %md
# MAGIC Create a config dictionary to define the connection to Cosmos.

# COMMAND ----------

if cosmos_read_write_key:
  masterkey = cosmos_read_write_key
else:
  masterkey = cosmos_read_key

cosmosConfig = {
  "Endpoint": cosmos_uri,
  "Masterkey": masterkey,
  "Database": cosmos_database,
  "Collection": cosmos_container
}

# COMMAND ----------

# MAGIC %md
# MAGIC Now check the connection to Cosmos DB.

# COMMAND ----------

try:
  dfCosmos = (spark.read
    .format("com.microsoft.azure.cosmosdb.spark")
    .options(**cosmosConfig)
    .load()
  )
except Exception as e:
  print(e)
  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Store Results in Cosmos DB
# MAGIC 
# MAGIC After performing inference using Spark, we can write out the results to a database so that the results can be quickly served.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This code parallelizes the database write by creating one database connection for each partition of the data.  You can control the data partitions using `.repartition()` or `.coalesce()`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Write out `predictionsDF` to the database.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> This is commented out because it will not work as is.  You need to provide read/write credentials to your own database [in this notebook]($./Includes/Azure-Credentials ).  You also need to re-run the classroom setups script at the top this notebook to put those changes into effect.

# COMMAND ----------

# (predictionDF.write
#   .mode("overwrite")
#   .format("com.microsoft.azure.cosmosdb.spark")
#   .options(**cosmosConfig)
#   .save())

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm the write by reading the data back from Cosmos.

# COMMAND ----------

dfCosmos = (spark.read
  .format("com.microsoft.azure.cosmosdb.spark")
  .options(**cosmosConfig)
  .load())

display(dfCosmos)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> **Please be sure to delete any infrastructure you build after the course so you don't incur unexpected expenses.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Other Deployment Options
# MAGIC 
# MAGIC There are a number of other common batch deployment options.  One common use case is going from a Python environment for training to a Java environment for deployment.  Here are a few tools:<br><br>
# MAGIC 
# MAGIC  - **An Easy Port to Java:** In certain models, such as linear regression, the coefficients of a trained model can be taken and implemented by hand in Java.  This can work with tree-based models as well.
# MAGIC  - **Re-serializing for Java:** Since Python uses Pickle by default to serialize, a library like <a href="https://github.com/jpmml/jpmml-sklearn" target="_blank">jpmml-sklearn</a> can de-serialize `sklearn` libraries and re-serialize them for use in Java environments.
# MAGIC  - **Leveraging Library Functionality:** Some libraries include the ability to deploy to Java such as <a href="https://github.com/dmlc/xgboost/tree/master/jvm-packages" target="_blank">xgboost4j</a>.
# MAGIC  - **Containers:** Containerized solutions are becoming increasingly popular since they offer the encapsulation and reliability offered by jars while offering more deployment options than just the Java environment.
# MAGIC 
# MAGIC Finally, <a href="http://mleap-docs.combust.ml/" target="_blank">MLeap</a> is a common, open source serialization format and execution engine for Spark, `sklearn`, and `TensorFlow`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** What are the main considerations in batch deployments?
# MAGIC **Answer:** The following considerations help determine the best way to deploy batch inference results:
# MAGIC * How the data will be queried
# MAGIC * How the data will be written
# MAGIC * The training and deployment environment
# MAGIC * What data the final model is trained on
# MAGIC 
# MAGIC **Question:** How can you optimize inference reads and writes?
# MAGIC **Answer:** Writes can be optimized by managing parallelism.  In Spark, this would mean managing the partitions of a DataFrame such that work is evenly distributed and you have the most efficient connections back to the target database.
# MAGIC 
# MAGIC **Question:** How can I deploy models trained in Python in a Java environment?
# MAGIC **Answer:** There are a number of ways to do this.  It's not unreasonable to just export model coefficients or trees in a random forest and parse them in Java.  This works well as a minimum viable product.  You can also look at different libraries that can serialize models in a way that the JVM can make use of them.  `jpmml-sklearn` and `xgboost4j` are two examples of this.  Finally, you can re-implement Python libraries in Java if needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Lab<br>
# MAGIC 
# MAGIC ### [Click here to start the lab for this lesson.]($./Labs/03-Lab)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find more information on UDF's created by MLflow?
# MAGIC **A:** See the <a href="https://www.mlflow.org/docs/latest/python_api/mlflow.pyfunc.html" target="_blank">MLflow documentation for details</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>