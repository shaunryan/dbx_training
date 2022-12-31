# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming Deployment
# MAGIC 
# MAGIC After batch deployment, continuous model inference using a technology like Spark Streaming represents the second most common deployment option.  This lesson introduces how to perform inference on a stream of incoming data.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Make predictions on streaming data
# MAGIC  - Predict using an `sklearn` model on a stream of data
# MAGIC  - Stream predictions into an always up-to-date delta file
# MAGIC  - Create an Event Hubs endpoint
# MAGIC  - Stream data to and from Event Hubs

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Inference on Streaming Data
# MAGIC 
# MAGIC Spark Streaming enables...<br><br>
# MAGIC 
# MAGIC * Scalable and fault-tolerant operations that continuously perform inference on incoming data
# MAGIC * Streaming applications can also incorporate ETL and other Spark features to trigger actions in real time
# MAGIC 
# MAGIC This lesson is meant as an introduction to streaming applications as they pertain to production machine learning jobs.  
# MAGIC 
# MAGIC Streaming poses a number of specific obstacles. These obstacles include:<br><br>
# MAGIC 
# MAGIC * *End-to-end reliability and correctness:* Applications must be resilient to failures of any element of the pipeline caused by network issues, traffic spikes, and/or hardware malfunctions.
# MAGIC * *Handle complex transformations:* Applications receive many data formats that often involve complex business logic.
# MAGIC * *Late and out-of-order data:* Network issues can result in data that arrives late and out of its intended order.
# MAGIC * *Integrate with other systems:* Applications must integrate with the rest of a data infrastructure.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Streaming data sources in Spark...<br><br>
# MAGIC 
# MAGIC * Offer the same DataFrames API for interacting with your data
# MAGIC * The crucial difference is that in structured streaming, the DataFrame is unbounded
# MAGIC * In other words, data arrives in an input stream and new records are appended to the input DataFrame
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-3/structured-streamining-model.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 
# MAGIC Spark is a good solution for...<br><br>
# MAGIC 
# MAGIC * Batch inference
# MAGIC * Incoming streams of data
# MAGIC 
# MAGIC For low-latency inference, however, Spark may or may not be the best solution depending on the latency demands of your task

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Connecting to the Stream
# MAGIC 
# MAGIC As data technology matures, the industry has been converging on a set of technologies.  Apache Kafka and the Azure managed alternative Event Hubs have become the ingestion engine at the heart of many pipelines.  
# MAGIC 
# MAGIC This technology brokers messages between producers, such as an IoT device writing data, and consumers, such as a Spark cluster reading data to perform real time analytics. There can be a many-to-many relationship between producers and consumers and the broker itself is scalable and fault tolerant.
# MAGIC 
# MAGIC We'll simulate a stream using the `maxFilesPerTrigger` option before creating an Azure Event Hubs endpoint and using this as our streaming data source.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/>  There are a number of ways to stream data.  One other common design pattern is to stream from an Azure Blob Container where any new files that appear will be read by the stream.

# COMMAND ----------

# MAGIC %md
# MAGIC Import the dataset in Spark.

# COMMAND ----------

airbnbDF = spark.read.parquet("/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.parquet/")

display(airbnbDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a schema for the data stream.  Data streams need a schema defined in advance.

# COMMAND ----------

from pyspark.sql.types import DoubleType, IntegerType, StructType

schema = (StructType()
.add("host_total_listings_count", DoubleType())
.add("neighbourhood_cleansed", IntegerType())
.add("zipcode", IntegerType())
.add("latitude", DoubleType())
.add("longitude", DoubleType())
.add("property_type", IntegerType())
.add("room_type", IntegerType())
.add("accommodates", DoubleType())
.add("bathrooms", DoubleType())
.add("bedrooms", DoubleType())
.add("beds", DoubleType())
.add("bed_type", IntegerType())
.add("minimum_nights", DoubleType())
.add("number_of_reviews", DoubleType())
.add("review_scores_rating", DoubleType())
.add("review_scores_accuracy", DoubleType())
.add("review_scores_cleanliness", DoubleType())
.add("review_scores_checkin", DoubleType())
.add("review_scores_communication", DoubleType())
.add("review_scores_location", DoubleType())
.add("review_scores_value", DoubleType())
.add("price", DoubleType())
)

# COMMAND ----------

# MAGIC %md
# MAGIC Check to make sure the schemas match.

# COMMAND ----------

schema == airbnbDF.schema

# COMMAND ----------

# MAGIC %md
# MAGIC Check the number of shuffle partitions.

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC Change this to 8.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC Create a data stream using `readStream` and `maxFilesPerTrigger`.

# COMMAND ----------

streamingData = (spark
                 .readStream
                 .schema(schema)
                 .option("maxFilesPerTrigger", 1)
                 .parquet("/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.parquet/")
                 .drop("price"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply an `sklearn` Model on the Stream
# MAGIC 
# MAGIC Using the DataFrame API, Spark allows us to interact with a stream of incoming data in much the same way that we did with a batch of data.  

# COMMAND ----------

# MAGIC %md
# MAGIC Import a `spark_udf`

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.ensemble import RandomForestRegressor

with mlflow.start_run(run_name="Final RF Model") as run: 
  df = pd.read_csv("/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv")
  X = df.drop(["price"], axis=1)
  y = df["price"]

  rf = RandomForestRegressor(n_estimators=100, max_depth=5)
  rf.fit(X, y)
  
  mlflow.sklearn.log_model(rf, "random-forest-model")
  
  runID = run.info.run_uuid
  experimentId = run.info.experiment_id
  URI = run.info.artifact_uri

# COMMAND ----------

# MAGIC %md
# MAGIC Create a UDF from the model you just trained in `sklearn` so that you can apply it in Spark.

# COMMAND ----------

import mlflow.pyfunc

pyfunc_udf = mlflow.pyfunc.spark_udf(spark, URI + "/random-forest-model")

# COMMAND ----------

# MAGIC %md
# MAGIC Transform the stream with a prediction.

# COMMAND ----------

predictionsDF = streamingData.withColumn("prediction", pyfunc_udf(*streamingData.columns))

display(predictionsDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Write out a Stream of Predictions
# MAGIC 
# MAGIC You can perform writes to any target database.  In this case, write to a Delta file.  This file will always be up to date, another component of an application can query this endpoint at any time.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Delta is an open-source storage layer that brings ACID transactions to Spark and big data workloads.  It is built on the Parquet format.  <a href="https://databricks.com/product/databricks-delta" target="_blank">Find out more about Delta here.</a>

# COMMAND ----------

dbutils.fs.rm(userhome + "ml-deployment", True)
checkpointLocation = userhome + "/ml-deployment/stream.checkpoint"
writePath = userhome + "/ml-deployment/predictions"

(predictionsDF
  .writeStream                                           # Write the stream
  .format("delta")                                       # Use the delta format
  .partitionBy("zipcode")                                # Specify a feature to partition on
  .option("checkpointLocation", checkpointLocation)      # Specify where to log metadata
  .option("path", writePath)                             # Specify the output path
  .outputMode("append")                                  # Append new records to the output path
  .start()                                               # Start the operation
)

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the underlying file.  Refresh this a few times.

# COMMAND ----------

from pyspark.sql.utils import AnalysisException

try:
  print(spark.read.format("delta").load(writePath).count())
except AnalysisException:
  print("Files not found.  This could be because the stream hasn't initialized.  Try again in a moment.")

# COMMAND ----------

# MAGIC %md
# MAGIC Stop the stream.

# COMMAND ----------

[q.stop() for q in spark.streams.active]

# COMMAND ----------

# MAGIC %md
# MAGIC Things to note:<br><br>
# MAGIC 
# MAGIC * For batch processing, you can trigger a stream every 24 hours to maintain state.
# MAGIC * You can easily combine historic and new data in the same stream.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Working with Event Hub
# MAGIC 
# MAGIC This requires the following libraries:<br><br>
# MAGIC 
# MAGIC 1. The Java (Maven) library with coordinate `com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.7`
# MAGIC    - This allows Databricks `spark` session to communicate with an Event Hub.
# MAGIC    - <a href="https://files.training.databricks.com/static/step-by-step/installing-libraries-from-maven.html" target="_blank">See the instructions on how to install a library from Maven</a> if you're unfamiliar with the process.
# MAGIC 2. The Python (PyPi) library `azure-eventhub`
# MAGIC    - This allows the Python kernel to stream content to an Event Hub.
# MAGIC    - <a href="https://files.training.databricks.com/static/step-by-step/installing-libraries-from-pypi.html" target="_blank">See the instructions on how to install a library from PyPi</a> if you're unfamiliar with the process.
# MAGIC    
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> **Please be sure to delete any infrastructure you build after the course so you don't incur unexpected expenses.**

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC To set up Event Hubs, do the following:<br><br>
# MAGIC 
# MAGIC 1. Access the Azure Portal via the link in Azure Databricks.
# MAGIC <div><img src="https://files.training.databricks.com/images/airlift/AAHy9d889ERM6rJd2US1kRiqGCLiHzgmtFsB.png" width=800px /></div>
# MAGIC 2. In the search bar, enter **Event Hub**, click on the **Event Hubs** result. <br></br>
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/eventhub1.png" style="height: 350px; margin: 20px"/></div>
# MAGIC 3. Click **Add**. <br></br>
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/eventhub2.png" style="height: 150px; margin: 20px"/></div>
# MAGIC 4. Enter `airbnb-data` for the **Name**, select a **pricing tier**, select your **Subscription** and **Resource Group** and select `West US` for **Location**. Then click create.  This creates the namespace, which will contain the Event Hub endpoint we'll use. <br></br>
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/eventhub3-update.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 5. Click on **Event Hubs** and then add an Event Hub.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/eventhub5.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 6. Call the event hub `listingstream` and then click **Create**.  This is the endpoint we'll be using within the `airbnb-data` namespace.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/eventhub6.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 7. Now create an access policy so that we can access this endpoint.  Click **Shared access policies**, name the policy `readwrite`, and send and listen rights.  Click **Create**.  Copy the connection string for the primary key.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/eventhub7.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC Fill the following cell out with your own details.

# COMMAND ----------

read_write_connection_string = "" 
read_only_connection_string = "Endpoint=sb://airbnb-data.servicebus.windows.net/;SharedAccessKeyName=readonly;SharedAccessKey=g4JJeAlVOl5Dx6Q3L3l9izAX2BPmnap546ytu9qRef8=;EntityPath=listingstream"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connecting to an Event Hub Stream
# MAGIC 
# MAGIC First clear the folders we'll be using in case there are preexisting files.

# COMMAND ----------

dbutils.fs.rm(userhome + "/academy/", recurse=True)
dbutils.fs.rm(userhome + "/event-hub", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC We will first create a stream from our data sitting at `/mnt/training`.  This gives us the data we need to stream into Event Hub.

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StringType, DoubleType, LongType

airbnbDF = spark.read.parquet("/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.parquet/").drop('price')
airbnbJson = spark.createDataFrame(airbnbDF.toJSON(), StringType()).withColumnRenamed("value", "body")

jsonURL = userhome + "/ml-deployment/event-hub"
airbnbJson.write.mode("overwrite").format("delta").save(jsonURL)

airbnbDFStream = (spark.readStream
  .format("delta")
  .option("maxFilesPerTrigger", 1)
  .load(jsonURL)
)
display(airbnbDFStream)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Now write this stream to Event Hub.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> You will need to provide your own read/write credentials to your own Event Hub for this to work.

# COMMAND ----------

if read_write_connection_string: 
  read_write_config = {
    'eventhubs.connectionString' : read_write_connection_string
  }

  (airbnbDFStream
    .writeStream
    .format("eventhubs")
    .options(**read_write_config)
    .option("checkpointLocation", userhome + "/event-hub/write-checkpoint")
    .start())
else:
  print("Read/Write connection string not found.  Skipping the write to Event Hub.")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Define a reading configuration so that you can read data from Event Hub.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This is using our read only keys.  You can change this to your own endpoint when you set up your infrastructure.

# COMMAND ----------

import json

# Create the starting position Dictionary
startingEventPosition = {
  "offset": "-1",
  "seqNo": -1,            # not in use
  "enqueuedTime": None,   # not in use
  "isInclusive": True
}

eventHubsConf = {
  "eventhubs.connectionString" : read_only_connection_string, # Change this to your own connection string if needed
  "eventhubs.startingPosition" : json.dumps(startingEventPosition),
  "setMaxEventsPerTrigger": 100
}

# COMMAND ----------

# MAGIC %md
# MAGIC Create a data stream using `readStream`.

# COMMAND ----------

from pyspark.sql.functions import col

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

eventStreamDF = (spark.readStream
  .format("eventhubs")
  .options(**eventHubsConf)
  .load()
  .select(col("body").cast("STRING"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a schema for the data stream.

# COMMAND ----------

from pyspark.sql.functions import col, from_json

schema = airbnbDF.schema

rawEventsDF = (eventStreamDF
  .select(from_json(col("body"), schema).alias("json"))
  .select("json.*")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Make sure the number of shuffle partitions is set to 8.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply the model on an Event Hub Stream
# MAGIC 
# MAGIC Using the 
# MAGIC DataFrame API, Spark allows us to interact with a stream of incoming data in much the same way that we did with a batch of data.  

# COMMAND ----------

# MAGIC %md
# MAGIC Transform the stream with a prediction.

# COMMAND ----------

predictionsDF = (rawEventsDF
  .withColumn("prediction", pyfunc_udf(*rawEventsDF.columns))
)

display(predictionsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Write the results to a delta table.

# COMMAND ----------

checkpointLocation = userhome + "/academy/stream.checkpoint"
writePath = userhome + "/academy/predictions"

(predictionsDF
  .writeStream                                           # Write the stream
  .format("delta")                                       # Use the delta format
  .partitionBy("zipcode")                                # Specify a feature to partition on
  .option("checkpointLocation", checkpointLocation)      # Specify where to log metadata
  .option("path", writePath)                             # Specify the output path
  .outputMode("append")                                  # Append new records to the output path
  .start()                                               # Start the operation
)

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the underlying file.  Refresh this a few times.

# COMMAND ----------

from pyspark.sql.utils import AnalysisException

try:
  print(spark.read.load(writePath).count())
except AnalysisException:
  print("Files not found.  This could be because the stream hasn't initialized.  Try again in a moment.")

# COMMAND ----------

# MAGIC %md
# MAGIC Stop the stream.

# COMMAND ----------

[q.stop() for q in spark.streams.active]

# COMMAND ----------

# MAGIC %md
# MAGIC Delete the files you copied to DBFS.

# COMMAND ----------

dbutils.fs.rm(writePath, True)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> **Please be sure to delete any infrastructure you build after the course so you don't incur unexpected expenses.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC 
# MAGIC **Question:** What are commonly approached as data streams?  
# MAGIC **Answer:** Apache Kafka and the Azure managed alternative Event Hubs are common data streams.  Additionally, it's common to monitor a directory for incoming files.  When a new file appears, it is brought into the stream for processing.
# MAGIC 
# MAGIC **Question:** How does Spark ensure exactly-once data delivery and maintain metadata on a stream?  
# MAGIC **Answer:** Checkpoints give Spark this fault tolerance through the ability to maintain state off of the cluster.
# MAGIC 
# MAGIC **Question:** How does the Spark approach to streaming integrate with other Spark features?  
# MAGIC **Answer:** Spark Streaming uses the same DataFrame API, allowing easy integration with other Spark functionality.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Lab<br>
# MAGIC 
# MAGIC ### [Click here to start the lab for this lesson.]($./Labs/04-Lab) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find out more information on streaming ETL jobs?  
# MAGIC **A:** Check out the Databricks blog post <a href="https://databricks.com/blog/2017/01/19/real-time-streaming-etl-structured-streaming-apache-spark-2-1.html" target="_blank">Real-time Streaming ETL with Structured Streaming in Apache Spark 2.1</a>
# MAGIC 
# MAGIC **Q:** Where can I get more information on integrating Streaming and Kafka?  
# MAGIC **A:** Check out the <a href="https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html" target="_blank">Structured Streaming + Kafka Integration Guide</a>
# MAGIC 
# MAGIC **Q:** Where can I see a case study on an IoT pipeline using Spark Streaming?  
# MAGIC **A:** Check out the Databricks blog post <a href="https://databricks.com/blog/2017/04/26/processing-data-in-apache-kafka-with-structured-streaming-in-apache-spark-2-2.html" target="_blank">Processing Data in Apache Kafka with Structured Streaming in Apache Spark 2.2</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>