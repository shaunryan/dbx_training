# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab: Reading from Cosmos vs Reading from Parquet
# MAGIC Storing predictions in different forms has a number of downstream effects, particularly on latency and throughput.  This lab compares reading from a database to reading from a static file sitting on a blob store.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lab you:<br>
# MAGIC  - Configure and use an Azure Cosmos database
# MAGIC  - Read from a parquet file
# MAGIC  - Compare runtimes of using Cosmos and Parquet

# COMMAND ----------

# MAGIC %run "./../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure and Use an Azure Cosmos Database
# MAGIC 
# MAGIC You will need to install the Azure connector from the Maven coordinates `com.microsoft.azure:azure-cosmosdb-spark_2.4.0_2.11:1.3.5`
# MAGIC 
# MAGIC <a href="https://files.training.databricks.com/static/step-by-step/installing-libraries-from-maven.html" target="_blank">See the instructions on how to install a library from Maven</a> if you're unfamiliar with the process.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to configure a Cosmos database to read our airbnb predictions from.

# COMMAND ----------

PrimaryRead = "YN7YcizZ3Q06UadXdESpsFvaZ1TJUqxBe4wXpH8A6eJUoMFe41kLSBbQx4yxwxbtdtLcDlvfkvHuO7la0XeV5A==" # Read only keys
Endpoint = "https://airbnbprediction.documents.azure.com:443/"
CosmosDatabase =  "predictions"
CosmosCollection = "predictions"

if not PrimaryRead:
  raise Exception("Don't forget to specify the cosmos keys in this cell.")

cosmosConfig = {
  "Endpoint": Endpoint,
  "Masterkey": PrimaryRead,
  "Database": CosmosDatabase,
  "Collection": CosmosCollection
}

# COMMAND ----------

# MAGIC %md
# MAGIC Create a Spark DataFrame to read from Cosmos DB to see its contents.

# COMMAND ----------

cosmos_prediction_df = (spark.read
    .format("com.microsoft.azure.cosmosdb.spark")
    .options(**cosmosConfig)
    .load()
   )
 
display(cosmos_prediction_df)

# COMMAND ----------

# MAGIC %md
# MAGIC This is the standard airbnb dataset with some additional metadata.
# MAGIC 
# MAGIC Based off of the above code to create a DataFrame from Cosmos DB, fill in the `predict_cosmos(id)` function to read from Cosmos DB and return the predicted price (a float type) based on the column `id`.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col

def predict_cosmos(id):
  # read in df from Cosmos
  prediction = (spark.read
    .format("com.microsoft.azure.cosmosdb.spark")
    .options(**cosmosConfig)
    .load()
    .filter(col("id") == id)
    .select("prediction")
    .first()
  )
  try:
    return prediction[0]
  except TypeError:
    return None

p = predict_cosmos("6d74a75b-63d8-4e0e-b95c-ce2e512d8e0a")

print(p)
assert type(p) == float, " `predict_cosmos` should return 1 float type"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading from a Parquet File

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Similar to the previous cell, fill in the `predict_parquet(id)` function to return the prediction of row `id` from the parquet file stored at path `/mnt/training/airbnb/sf-listings/prediction.parquet`.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Load in a parquet file path using `spark.read.parquet`.

# COMMAND ----------

# ANSWER

def predict_parquet(id):
  prediction = (spark.read
    .parquet("/mnt/training/airbnb/sf-listings/prediction.parquet")
    .filter(col("id") == id)
    .select("prediction")
    .first()
  )
  try:
    return prediction[0]
  except TypeError:
    return None

p = predict_parquet("7b22b1d3-c634-4bad-a854-17e0669aa685")
print(p)
assert type(p) == float, " `predict_parquet` should return 1 float type"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare Runtimes of Cosmos and Parquet
# MAGIC 
# MAGIC Run the following cells to look at the average time it takes to execute 5 different queries on from Cosmos versus a parquet file.

# COMMAND ----------

num_queries = 5

id_list = cosmos_prediction_df.limit(num_queries).select("id").collect()
ids = [i[0] for i in id_list]

ids

# COMMAND ----------

# MAGIC %timeit -n3 [predict_cosmos(id) for id in ids]

# COMMAND ----------

# MAGIC %timeit -n3 [predict_parquet(id) for id in ids]

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Which performed better?  What are the trade-offs between using the two solutions in production?
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> These results will depend in part on the load on Cosmos generated by the class.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Next Lesson<br>
# MAGIC 
# MAGIC ### Start the next lesson, [Streaming Deployment]($../04-Streaming-Deployment )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>