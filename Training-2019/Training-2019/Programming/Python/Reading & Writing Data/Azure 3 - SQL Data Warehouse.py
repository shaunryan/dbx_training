# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading and Writing to Azure SQL Data Warehouse
# MAGIC **Technical Accomplishments:**
# MAGIC - Access an Azure SQL Data Warehouse using the SQL Data Warehouse connector
# MAGIC 
# MAGIC **Requirements:**
# MAGIC - Databricks Runtime 4.0 or above
# MAGIC - A database master key for the Azure SQL Data Warehouse
# MAGIC 
# MAGIC You will create a data warehouse and database master key for it in the steps below.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Azure SQL Data Warehouse
# MAGIC Azure SQL Data Warehouse leverages massively parallel processing (MPP) to quickly run complex queries across petabytes of data.
# MAGIC 
# MAGIC Import big data into SQL Data Warehouse with simple PolyBase T-SQL queries, and then use MPP to run high-performance analytics.
# MAGIC 
# MAGIC As you integrate and analyze, the data warehouse will become the single version of truth your business can count on for insights.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) SQL Data Warehouse Connector
# MAGIC - Use Azure Blob Storage as an intermediary between Azure Databricks and SQL Data Warehouse
# MAGIC - In Azure Databricks: triggers Spark jobs to read and write data to Blob Storage
# MAGIC - In SQL Data Warehouse: triggers data loading and unloading operations, performed by **PolyBase**
# MAGIC 
# MAGIC **Note:** The SQL DW connector is more suited to ETL than to interactive queries.  
# MAGIC For interactive and ad-hoc queries, data should be extracted into a Databricks Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/adbcore/AAHRBWKzrNVMUpfjecWUpfRb9p8pVZl7fsMB.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Types of Connections in SQL Data Warehouse
# MAGIC 
# MAGIC ### **Spark Driver to SQL DW**
# MAGIC Spark driver connects to SQL Data Warehouse via JDBC using a username and password.
# MAGIC 
# MAGIC ### **Spark Driver and Executors to Azure Blob Storage**
# MAGIC Spark uses the **Azure Blob Storage connector** bundled in Databricks Runtime to connect to the Blob Storage container.
# MAGIC   - Requires **`wasbs`** URI scheme to specify connection
# MAGIC   - Requires **storage account access key** to set up connection
# MAGIC     - Set in a notebook's session configuration, which doesn't affect other notebooks attached to the same cluster
# MAGIC     - **`spark`** is the SparkSession object provided in the notebook
# MAGIC 
# MAGIC ### **SQL DW to Azure Blob Storage**
# MAGIC SQL DW connector forwards the access key from notebook session configuration to SQL Data Warehouse instance over JDBC.
# MAGIC   - Requires **`forwardSparkAzureStorageCredentials`** set to **`true`**
# MAGIC   - Represents access key with a temporary <a href="https://docs.microsoft.com/en-us/sql/t-sql/statements/create-database-scoped-credential-transact-sql?view=sql-server-2017" target="_blank">database scoped credential</a> in the SQL Data Warehouse instance
# MAGIC   - Creates a database scoped credential before asking SQL DW to load or unload data, and deletes after loading/unloading is finished

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Configuration
# MAGIC 
# MAGIC ### Create Azure Blob Storage
# MAGIC Follow these steps to <a href="https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account?tabs=azure-portal#regenerate-storage-access-keys" target="_blank">create an Azure Storage Account</a> and Container.  
# MAGIC The SQL DW connector will use a <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key" target="_blank">Shared Key</a> for authorization.
# MAGIC 
# MAGIC As you work through the following steps, record the **Storage Account Name**, **Container Name**, and **Access Key** in the cell below:
# MAGIC 0. Access the Azure Portal > Create a new resource > Storage account
# MAGIC 0. Specify the correct *Resource Group* and *Region*, and use any unique string for the **Storage Account Name**
# MAGIC 0. Access the new Storage account > Access Blobs
# MAGIC 0. Create a New Container using any unique string for the **Container Name**
# MAGIC 0. Retrieve the primary **Access Key** for the new Storage Account

# COMMAND ----------

# TODO
storageAccount = ""
containerName = ""
accessKey = ""

spark.conf.set("fs.azure.account.key.{}.blob.core.windows.net".format(storageAccount), accessKey)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Configuration
# MAGIC 
# MAGIC ### Create Azure SQL Data Warehouse
# MAGIC Follow these steps to <a href="https://docs.microsoft.com/en-us/azure/sql-data-warehouse/create-data-warehouse-portal" target="_blank">create an Azure SQL Data Warehouse</a>.
# MAGIC 
# MAGIC 0. Access the Azure Portal > Create a new resource > SQL Data Warehouse
# MAGIC 0. Specify the following attributes for the SQL Data Warehouse:
# MAGIC    - Use any string for the **Data warehouse name**
# MAGIC    - Select an existing or create a new SQL Server
# MAGIC    - Under the **Additional Settings** tab, select **Sample** for the **Use existing data** option
# MAGIC 0. Access the new SQL Data Warehouse
# MAGIC 0. Select **Query Editor (preview)** under **Common Tasks** in the sidebar and enter the proper credentials
# MAGIC 0. Run these two queries:
# MAGIC    - Create a Master Key in the SQL DW. This facilitates the SQL DW connection
# MAGIC    
# MAGIC      **`CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'CORRECT-horse-battery-staple';`**
# MAGIC 
# MAGIC    - Use a CTAS to create a staging table for the Customer Table. This query will create an empty table with the same schema as the Customer Table.
# MAGIC    
# MAGIC      **`CREATE TABLE dbo.DimCustomerStaging`**  
# MAGIC      **`WITH`**  
# MAGIC      **`( DISTRIBUTION = ROUND_ROBIN, CLUSTERED COLUMNSTORE INDEX )`**  
# MAGIC      **`AS`**  
# MAGIC      **`SELECT  *`**  
# MAGIC      **`FROM dbo.DimCustomer`**  
# MAGIC      **`WHERE 1 = 2`**  
# MAGIC      **`;`**
# MAGIC 
# MAGIC 0. Access Connection Strings
# MAGIC 0. Select JDBC and copy the **JDBC URI**

# COMMAND ----------

# TODO
tableName = "dbo.DimCustomer"
jdbcURI = ""

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Read from the Customer Table
# MAGIC 
# MAGIC Use the SQL DW Connector to read data from the Customer Table.
# MAGIC 
# MAGIC Use the read to define a tempory table that can be queried.
# MAGIC 
# MAGIC Note the following options in the DataFrameReader in the cell below:
# MAGIC * **`url`** specifies the JDBC connection to the SQL Data Warehouse
# MAGIC * **`tempDir`** specifies the **`wasbs`** URI of the caching directory on the Azure Blob Storage container
# MAGIC * **`forwardSparkAzureStorageCredentials`** is set to **`true`** to ensure that the Azure storage account access keys are forwarded from the notebook's session configuration to the SQL Data Warehouse

# COMMAND ----------

cacheDir = "wasbs://{}@{}.blob.core.windows.net/cacheDir".format(containerName, storageAccount)

customerDF = (spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcURI)
  .option("tempDir", cacheDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tableName)
  .load())

customerDF.createOrReplaceTempView("customer_data")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Use SQL queries to count the number of rows in the Customer table and to display table metadata.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Note that **`CustomerKey`** and **`CustomerAlternateKey`** use a very similar naming convention.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC When merging many new customers into this table, we may have issues with uniqueness in the **`CustomerKey`**. 
# MAGIC 
# MAGIC Let's redefine **`CustomerAlternateKey`** for stronger uniqueness using a <a href="https://en.wikipedia.org/wiki/Universally_unique_identifier" target="_blank">UUID</a>. To do this, we will define a UDF and use it to transform the **`CustomerAlternateKey`** column.

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import uuid

uuidUdf = udf(lambda : str(uuid.uuid4()), StringType())
customerUpdatedDF = customerDF.withColumn("CustomerAlternateKey", uuidUdf())
display(customerUpdatedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Use the Polybase Connector to Write to the Staging Table
# MAGIC 
# MAGIC Use the SQL DW Connector to write the updated customer table to a staging table.
# MAGIC 
# MAGIC It is best practice to update the SQL Data Warehouse via a staging table.
# MAGIC 
# MAGIC Note the following options in the DataFrameWriter in the cell below:
# MAGIC * **`url`** specifies the JDBC connection to the SQL Data Warehouse
# MAGIC * **`tempDir`** specifies the **`wasbs`** URI of the caching directory on the Azure Blob Storage container
# MAGIC * **`forwardSparkAzureStorageCredentials`** is set to **`true`** to ensure that the Azure storage account access keys are forwarded from the notebook's session configuration to the SQL Data Warehouse
# MAGIC 
# MAGIC These options are the same as those in the DataFrameReader above.

# COMMAND ----------

(customerUpdatedDF.write
  .format("com.databricks.spark.sqldw")
  .mode("overwrite")
  .option("url", jdbcURI)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbtable", tableName + "Staging")
  .option("tempdir", cacheDir)
  .save())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Read From the New Staging Table
# MAGIC Use the SQL DW Connector to read the new table we just wrote.
# MAGIC 
# MAGIC Use the read to define a tempory table that can be queried.

# COMMAND ----------

customerTempDF = (spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcURI)
  .option("tempDir", cacheDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tableName + "Staging")
  .load())

customerTempDF.createOrReplaceTempView("customer_temp_data")


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>