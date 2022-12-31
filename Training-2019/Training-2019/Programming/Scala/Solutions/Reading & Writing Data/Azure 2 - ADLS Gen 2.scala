// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Reading and Writing Data - Azure Data Lake Storage Gen2
// MAGIC **Technical Accomplishments:**
// MAGIC - Access an Azure Data Lake Storage Gen2 filesystem by mounting to DBFS
// MAGIC 
// MAGIC **Requirements:**
// MAGIC - Databricks Runtime 5.2 or above
// MAGIC - ADLS Gen2 storage account in the same region as your Azure Databricks workspace
// MAGIC - A service principal with delegated permissions OR storage account access key
// MAGIC 
// MAGIC You will create Gen2 storage account and access it using a service principal in the steps below.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Azure Data Lake Storage Gen2 (ADLS Gen2)
// MAGIC 
// MAGIC Azure Data Lake Storage Gen2 is a next-generation data lake solution for big data analytics.
// MAGIC 
// MAGIC ADLS Gen2 builds ADLS Gen1 capabilities - such as file system semantics, file-level security, and scale - 
// MAGIC 
// MAGIC into Azure Blob Storage, with its low-cost tiered storage, high availability, and disaster recovery features.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Create Service Principal
// MAGIC You can access an ADLS Gen2 filesystem by using a s**ervice principal** or by using the **storage account access key** directly.
// MAGIC 
// MAGIC However, if you want to mount the filesystem to DBFS, this requires OAuth 2.0 authentication, which means you'll have to use a **service principal**.
// MAGIC 
// MAGIC As you work through the following steps, record the **Directory ID**, **Application ID**, and **Secret** in the cell below:
// MAGIC 1. In Azure Active Directory, go to Properties. Note the **Directory ID**
// MAGIC 1. Go to App Registrations and create a new application registration
// MAGIC    * e.g. airlift-app-registration, Web app/API, https://can-be-literally-anything.com
// MAGIC 1. Note the **Application ID**
// MAGIC 1. Go to Certificates & Secrets and create a new client **secret** and copy its value

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Create Storage Account
// MAGIC 
// MAGIC Follow these steps to <a href="https://docs.microsoft.com/en-us/azure/storage/data-lake-storage/quickstart-create-account" target="_blank">create your ADLS Gen2 storage account</a>.
// MAGIC 
// MAGIC As you work through the following steps, record the **Storage Account Name** and **File System Name** in the cell below:
// MAGIC 0. Access the Azure Portal > Create a new resource > Storage account
// MAGIC 0. Make sure to specify the correct *Resource Group* and *Region*, and use any unique string for the **Storage Account Name**  
// MAGIC   - Ensure the storage account is in the same region as your Azure Databricks workspace
// MAGIC 0. Go to the **Advanced Tab** and **enable Hierarchal NameSpace**
// MAGIC 0. Create a Data Lake Gen2 file system on the storage account and enter the **File System Name** in the cell below.
// MAGIC 0. Under Access control (IAM) add a **Role assignment**, where the role is **Storage Blob Data Contributor (Preview)** assigned to the App Registration previously created

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Mount ADLS Gen2 filesystem to DBFS
// MAGIC 
// MAGIC Use **`dbutils.fs.mount`** to <a href="https://docs.databricks.com/spark/latest/data-sources/azure/azure-datalake-gen2.html#mount-an-adls-filesystem-to-dbfs-using-a-service-principal-and-oauth-2-0" target="_blank">mount this filesystem</a> to DBFS.

// COMMAND ----------

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> applicationID,
  "fs.azure.account.oauth2.client.secret" -> keyValue,
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/%s/oauth2/token".format(directoryID))

dbutils.fs.mount(
  source = "abfss://%s@%s.dfs.core.windows.net/".format(fileSystemName, storageAccountName),
  mountPoint = "/mnt/%s-%s".format(username, fileSystemName),
  extraConfigs = configs)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Access Your Files
// MAGIC Now, you can access files in your container as if they were local files!

// COMMAND ----------

val files = dbutils.fs.ls("/mnt/%s-%s".format(username, fileSystemName))
display(files)

// COMMAND ----------

val wikiEditsDF = spark.read.json("/mnt/%s-%s/wikipedia/edits/snapshot-2016-05-26.json".format(username, fileSystemName))
display(wikiEditsDF)

// COMMAND ----------

wikiEditsDF.write
  .mode("overwrite")
  .format("delta")
  .partitionBy("channel")
  .save("/mnt/%s-%s/wikiEdits".format(username, fileSystemName))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Unmount a Mount Point
// MAGIC Use **`dbutils.fs.unmount`** to unmount a mount point.

// COMMAND ----------

dbutils.fs.unmount("/mnt/%s-%s".format(username, fileSystemName))

// COMMAND ----------



// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>