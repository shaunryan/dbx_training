# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab: Querying from AzureML vs Parquet
# MAGIC REST endpoints offer a performant way of serving pre-calculated predictions or creating predictions in real time.  This lab compares the latency of quering a REST endpoint versus a static Parquet file.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lab you:<br>
# MAGIC  - Connect to an Azure ML REST Endpoint
# MAGIC  - Define connection helper functions
# MAGIC  - Query a Parquet file
# MAGIC  - Conduct time comparisons between the 2 methods

# COMMAND ----------

# MAGIC %run "./../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to an Endpoint
# MAGIC 
# MAGIC You can accomplish this by either connecting with your own or Databricks infrastructure.  In either case, start by pulling the URI and key.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Option 1: Use Databricks Infrastructure
# MAGIC 
# MAGIC Run the following cell for the URI and key you need for the REST endpoint.

# COMMAND ----------

# Databricks read only keys
prod_scoring_uri = "http://airbnb-ml-inference.databricks.training/api/v1/service/airbnb-model/score"
prod_service_key = "hqucurdsmp83JcW30N9zPwWvzg1dG9eq"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Option 2: Use Your Own Infrastructure
# MAGIC 
# MAGIC Fill out your workspace name, location, resource group, and subscription ID.

# COMMAND ----------

# workspace_name = ""
# workspace_location = ""
# resource_group = ""
# subscription_id = ""

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following to pull your webservice information.  You might have to modify this to match your specific deployment.  You can use AKS or ACI.

# COMMAND ----------

# import azureml
# from azureml.core import Webservice, Workspace

# workspace = Workspace.create(
#   name = workspace_name,
#   location = workspace_location,
#   resource_group = resource_group,
#   subscription_id = subscription_id,
#   exist_ok=True
# )
# print(workspace.webservices.keys())

# # dev_webservice = Webservice(workspace, "airbnb-model-dev") # You can use the ACI endpoint as well
# prod_webservice = Webservice(workspace, "airbnb-model")

# # assert dev_webservice.state == "Healthy"
# assert prod_webservice.state == "Healthy"

# prod_scoring_uri = prod_webservice.scoring_uri
# prod_service_key = prod_webservice.get_keys()[0] if len(prod_webservice.get_keys()) > 0 else None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Helper Functions
# MAGIC 
# MAGIC Take a look at the following sample input.  It includes column names and a number of different sample data points.  The string that prints out is what will be passed to the REST endpoint.

# COMMAND ----------

import json

sample_input = {
  'columns': [
    'host_total_listings_count', 
    'neighbourhood_cleansed', 
    'zipcode', 
    'latitude', 
    'longitude', 
    'property_type', 
    'room_type', 
    'accommodates', 
    'bathrooms', 
    'bedrooms', 
    'beds', 
    'bed_type', 
    'minimum_nights', 
    'number_of_reviews', 
    'review_scores_rating', 
    'review_scores_accuracy', 
    'review_scores_cleanliness', 
    'review_scores_checkin', 
    'review_scores_communication', 
    'review_scores_location', 
    'review_scores_value'
  ], 
  'data': [
    [1.0, 0, 0, 37.7693103773, -122.4338563449, 0, 0, 3.0, 1.0, 1.0, 2.0, 0, 1.0, 127.0, 97.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0],
    [4.0, 3, 2, 37.7307459298, -122.4484086264, 1, 1, 1.0, 2.0, 1.0, 1.0, 0, 3.0, 76.0, 95.0, 9.0, 9.0, 10.0, 10.0, 9.0, 9.0],
    [10.0, 2, 0, 37.7666895979, -122.4525046176, 0, 1, 2.0, 4.0, 1.0, 1.0, 0, 32.0, 17.0, 85.0, 8.0, 8.0, 9.0, 9.0, 9.0, 8.0]
  ]}

json.dumps(sample_input)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a helper function that takes `sample_input` and returns a list of predictions from a rest endpoint.

# COMMAND ----------

# TODO
def query_endpoint(scoring_uri, inputs, service_key=None):
  # FILL_IN
  
query_endpoint(prod_scoring_uri, sample_input, prod_service_key) # Use the function this way

# COMMAND ----------

# MAGIC %md
# MAGIC Now create another function that uses `n` number of samples to query the Azure ML endpoint.

# COMMAND ----------

# TODO
import pandas as pd

df = pd.read_csv("/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv").drop(["price"], axis=1)

def n_samples_azureML(n, df=df):
  # FILL_IN

n_samples_azureML(4) # Should return 4 samples

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying a Parquet File
# MAGIC 
# MAGIC Now create a similar function to query a Parquet file instead of our REST endpoint.

# COMMAND ----------

# MAGIC %md
# MAGIC Complete the following helper function `random_n_samples_parquet` to return `n` random queries from `predictions_df`.

# COMMAND ----------

# TODO
from pyspark.sql.functions import col

prediction_df = spark.read.parquet("/mnt/training/airbnb/sf-listings/prediction.parquet") 

def random_n_samples_parquet(n, df=prediction_df):

  # get n ids to query using
  id_list = df.limit(n).select("id").collect()
  query_ids = [i[0] for i in id_list]
  
  # get prediction of each row of 'query_ids'
  preds = []
  for i in query_ids:
    FILL_IN
    preds.append(pred)
    
  return preds

random_n_samples_parquet(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time Comparisons
# MAGIC 
# MAGIC Let's compare the time it takes to get the result of a single query from Azure ML and from a parquet file.

# COMMAND ----------

batch_size = 1

# COMMAND ----------

# MAGIC %timeit -n5 n_samples_azureML(batch_size)

# COMMAND ----------

# MAGIC %timeit -n5 random_n_samples_parquet(batch_size)

# COMMAND ----------

# MAGIC %md
# MAGIC But what if we want to ask multiple queries? Increase `batch_size` up to 30 and see which method is faster with a larger number of queries.

# COMMAND ----------

batch_size = 30

# COMMAND ----------

# MAGIC %timeit -n3 n_samples_azureML(batch_size)

# COMMAND ----------

# MAGIC %timeit -n5 random_n_samples_parquet(batch_size)

# COMMAND ----------

# MAGIC %md
# MAGIC Which performed better?  What are the trade-offs between using the two in production?

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Next Lesson<br>
# MAGIC 
# MAGIC ### Start the next lesson, [Drift Monitoring]($../06-Drift-Monitoring )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>