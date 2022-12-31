// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Reading and Writing Data - Azure Blob Storage
// MAGIC **Technical Accomplishments:**
// MAGIC - Access Azure Blob Storage directly using the DataFrame API
// MAGIC - Access Azure Blob Storage by mounting storage with DBFS
// MAGIC 
// MAGIC **Requirements:**
// MAGIC - Databricks Runtime 3.1 or above
// MAGIC - A Shared Key or Shared Access Signature (for accessing *private* storage accounts)
// MAGIC 
// MAGIC You will configure a Shared Key and Shared Access Signature (SAS) in the steps below.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Azure Blob Storage
// MAGIC Azure Blob Storage is a service for storing large amounts of unstructured object data, such as text or binary data.
// MAGIC 
// MAGIC You can use Blob Storage to expose data publicly to the world, or to store application data privately.
// MAGIC 
// MAGIC We will cover two ways you can access files from Azure Blob Storage here.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Method 1: Access Azure Blob Storage Directly
// MAGIC 
// MAGIC It is possible to read directly from Azure Blob Storage using the <a href="https://docs.databricks.com/spark/latest/data-sources/azure/azure-storage.html#access-azure-blob-storage-using-the-dataframe-api" target="_blank">Spark API and Databricks APIs</a>.
// MAGIC 
// MAGIC ### Configure Access to a Container Using a Shared Access Signature (SAS)
// MAGIC 
// MAGIC A <a href="https://docs.microsoft.com/en-us/azure/storage/common/storage-dotnet-shared-access-signature-part-1" target="_blank">Shared Access Signature (SAS)</a> is a great way to grant limited access to objects in your storage account to other clients, without exposing your account key.
// MAGIC 
// MAGIC Run the cell below to setup a SAS in your notebook by configuring the <a href="https://databricks.com/blog/2016/08/15/how-to-use-sparksession-in-apache-spark-2-0.html" target="_blank">SparkSession</a>.
// MAGIC 
// MAGIC In this example, we provide you with a SAS and storage container. Normally, you'll get these values from Azure, and then configure your SparkSession with these values.

// COMMAND ----------

val (source, sasEntity, sasToken) = getAzureDataSource()

spark.conf.set(sasEntity, sasToken)

displayHTML(s"""
Retrieved the following values:
  <li><b style="color:green">source:</b> <code>%s</code></li>
  <li><b style="color:green">sasEntity:</b> <code>%s</code></li>
  <li><b style="color:green">sasToken:</b> <code>%s</code></li><br>

Successfully set up a Shared Access Signature (SAS) in your notebook.
""".format(source, sasEntity, sasToken))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Read Data from Storage Account
// MAGIC You can now use standard Spark and Databricks APIs to read from the storage account.

// COMMAND ----------

display(dbutils.fs.ls(source))

// COMMAND ----------

val path = source + "wikipedia/edits/snapshot-2016-05-26.json"
val wikiEditsDF = spark.read.json(path)
display(wikiEditsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Method 2: Mount Azure Blob Storage with DBFS
// MAGIC 
// MAGIC Another way to access your Azure Blob Storage is to mount your storage with DBFS.
// MAGIC 
// MAGIC For this example, you'll create your own Azure <a href="https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account?tabs=azure-portal" target="_blank">Storage Account</a> and <a href="https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal" target="_blank">Container</a>.
// MAGIC 
// MAGIC This time, let's use a <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key" target="_blank">Shared Key</a> to authorize requests to the the Storage Account.
// MAGIC 
// MAGIC ### Create an Azure Storage Account and Container
// MAGIC As you work through the following steps, record the **Storage Account Name**, **Container Name**, and **Access Key** in the cell below:
// MAGIC 0. Access the Azure Portal > Create a new resource > Storage account
// MAGIC 0. Specify the correct *Resource Group* and *Region*, and use any unique string for the **Storage Account Name**
// MAGIC 0. Access the new Storage account > Access Blobs
// MAGIC 0. Create a New Container using any unique string for the **Container Name**
// MAGIC 0. Retrieve the primary **Access Key** for the new Storage Account

// COMMAND ----------

// MAGIC %md
// MAGIC ### Mount Container Using DBFS
// MAGIC Use **`dbutils.fs.mount`** to <a href="https://docs.databricks.com/spark/latest/data-sources/azure/azure-storage.html#mount-azure-blob-storage-containers-with-dbfs" target="_blank">mount this container</a> (or a folder inside this container) to DBFS.

// COMMAND ----------

val sourceURI = "wasbs://%s@%s.blob.core.windows.net/".format(containerName, storageAccountName)
val mountPath = "/mnt/%s-%s".format(username, containerName)

dbutils.fs.mount(
  source = sourceURI, 
  mountPoint = mountPath, 
  extraConfigs = Map(storageEntity -> accessKey))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Read and Write Data to Storage Account with DBFS
// MAGIC Now, you can access files in your container as if they were local files!

// COMMAND ----------

val files = dbutils.fs.ls("/mnt/%s-%s".format(username, containerName))
display(files)

// COMMAND ----------

// MAGIC %md
// MAGIC Writing to this mount point will write to your storage account.

// COMMAND ----------

val path = "%s/azblob/wikiEdits.parquet".format(userhome)

wikiEditsDF
  .write
  .mode("overwrite")
  .parquet(path)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Unmount a Mount Point
// MAGIC Use **`dbutils.fs.unmount`** to unmount a mount point.

// COMMAND ----------

dbutils.fs.unmount("/mnt/%s-%s".format(username, containerName))

// COMMAND ----------

display(dbutils.fs.mounts())

// COMMAND ----------



// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>