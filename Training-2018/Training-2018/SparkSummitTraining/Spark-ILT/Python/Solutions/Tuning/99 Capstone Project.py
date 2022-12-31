# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Capstone Project
# MAGIC 
# MAGIC **Dataset:**
# MAGIC * This lab uses a synthetic data generated specifically for these exercises
# MAGIC * Each year's of data is roughly the same with some variation for market growth in terms of sales volume
# MAGIC * We are looking at retail purchases from the top 100 retailers

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Your Personal Cluster
# MAGIC 
# MAGIC If you cluster already exists, just verify the settings are correct - edit & restart if necissary.
# MAGIC 
# MAGIC If your cluster does not yet exist, create your cluster as outlined below:

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC **Standard Configuration:**
# MAGIC * **Cluster Name**: Use your first name or pick a nickname for yourself. Avoid initials.
# MAGIC * **Cluster Mode**: Select <b style="color:blue">Standard</b>
# MAGIC * **Databricks Runtime Version**: Select the latest version, **unless instructed otherwise**
# MAGIC * **Python Version**: Select Python <b style="color:blue">3</b>
# MAGIC * **Driver Type**: Select <b style="color:blue">Same as worker</b>
# MAGIC * **Worker Type**: Select <b style="color:blue">Standard_D3_v2</b>.
# MAGIC   * <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> The default value looks very similar!
# MAGIC * **Enable autoscaling**: <b style="color:blue">Disable</b>, or rather, uncheck
# MAGIC * **Workers**: Please select only <b style="color:blue">2</b> workers. Selecting more will prevent the labs from functioning peoperly
# MAGIC * **Auto Termination**: <b style="color:blue">120 minutes</b>
# MAGIC 
# MAGIC This should yield a <b style="color:blue">42 GB</b> cluster with <b style="color:blue">8 cores</b>.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %run "../Includes/Initialize Labs"

# COMMAND ----------

# MAGIC %run "../Includes/Utility Methods"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Ready to go?
# MAGIC 
# MAGIC Let's make sure we are running with the expected 8 cores:

# COMMAND ----------

clearYourResults(False)
validateYourAnswer("00) Only 8 Cores", expectedHash=1276280174, answer=sc.defaultParallelism)
summarizeYourResults()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Introduction
# MAGIC 
# MAGIC We have [fictional] retail data inclusive of the years 2011 through 2018.
# MAGIC 
# MAGIC The data has it's origins in a relational database, as a consequence we have one table per year, and a couple of lookup tables:
# MAGIC 
# MAGIC Years 2011 to 2018 were each processed by different individuals resulting in slightly different formats. For eample:
# MAGIC * 2012 was written as a CSV
# MAGIC * 2017 was not partitioned at all
# MAGIC * 2014 and 2018 however, were partitioned by year and month
# MAGIC 
# MAGIC These files can be found in **"/mnt/training/global-sales/transactions"**:
# MAGIC * **dbfs:/mnt/training/global-sales/transactions/2011.parquet/**
# MAGIC * **dbfs:/mnt/training/global-sales/transactions/2012.csv/**
# MAGIC * **dbfs:/mnt/training/global-sales/transactions/2013.parquet/**
# MAGIC * **dbfs:/mnt/training/global-sales/transactions/2014.parquet/**
# MAGIC * **dbfs:/mnt/training/global-sales/transactions/2015.parquet/**
# MAGIC * **dbfs:/mnt/training/global-sales/transactions/2016.parquet/**
# MAGIC * **dbfs:/mnt/training/global-sales/transactions/2017.parquet/**
# MAGIC * **dbfs:/mnt/training/global-sales/transactions/2018.parquet/**
# MAGIC 
# MAGIC In addition to the transactional data, we have two lookup tables consisting of retailer and location information.
# MAGIC * **dbfs:/mnt/training/global-sales/cities/all.parquet/**	
# MAGIC * **dbfs:/mnt/training/global-sales/retailers/all.parquet/**
# MAGIC 
# MAGIC The goal of this exercise is two part:
# MAGIC   0. Answer some business questions about the 2011 to 2018 data.
# MAGIC   0. Clean the data up for future analysis with an eye to 2019 and 2020's data.
# MAGIC     * Optimize the data for better query performance
# MAGIC     * Create utility methods for code resuse
# MAGIC     
# MAGIC **Note:** The sequence of events outlined in this module are not necissarily the most effecient, just logical.   
# MAGIC It's up to you to implement the necissary shortcuts required to effectively work with "big data".

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #1
# MAGIC 
# MAGIC The first task is to load the schema:
# MAGIC * For each year of data, declare a DataFrame that reads in the Parquet or CSV file.
# MAGIC * The variable name for each DataFrame should take the form of **initDF_*YY*** where * **YY** * is the last two digits of the year.

# COMMAND ----------

# ANSWER

from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql.types import *

# COMMAND ----------

# ANSWER

# The file is stored as Parquet, but the 
# datatype of every column is String

schema_11 = "transacted_at string, trx_id string, retailer_id string, description string, amount string, city_id string"
initDF_11 = spark.read.schema(schema_11).parquet("dbfs:/mnt/training/global-sales/transactions/2011.parquet/")

# COMMAND ----------

# ANSWER

# The file is stored as CSV

schema_12 = "transacted_at string, trx_id string, retailer_id string, description string, amount string, city_id string"
initDF_12 = spark.read.schema(schema_12).csv("dbfs:/mnt/training/global-sales/transactions/2012.csv/")

# COMMAND ----------

# ANSWER

# Unpartitioned Parquet, but with tiny files (100 records per partition)

schema_13 = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer"
initDF_13 = spark.read.schema(schema_13).parquet("dbfs:/mnt/training/global-sales/transactions/2013.parquet/")

# COMMAND ----------

# ANSWER

# Parquet partitioned by year and month - small, but not tiny files.

schema_14 = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer, year integer, month integer"
initDF_14 = spark.read.schema(schema_14).parquet("dbfs:/mnt/training/global-sales/transactions/2014.parquet/")

# COMMAND ----------

# ANSWER

# Parquet Overpartitioned by year, month, day and hour.

schema_15 = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer, year integer, month integer, hour integer, day integer"
initDF_15 = spark.read.schema(schema_15).parquet("dbfs:/mnt/training/global-sales/transactions/2015.parquet/")

# COMMAND ----------

# ANSWER

# Parquet partioned by retailer_id

schema_16 = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer"
initDF_16 = spark.read.schema(schema_16).parquet("dbfs:/mnt/training/global-sales/transactions/2016.parquet/")

# COMMAND ----------

# ANSWER

# Unpartitioned parquet

schema_17 = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer"
initDF_17 = spark.read.schema(schema_17).parquet("dbfs:/mnt/training/global-sales/transactions/2017.parquet/")

# COMMAND ----------

# ANSWER

# Parquet partitioned by year and month - tiny files in November

schema_18 = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer, year integer, month integer"
initDF_18 = spark.read.schema(schema_18).parquet("dbfs:/mnt/training/global-sales/transactions/2018.parquet/")

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #1
# MAGIC 
# MAGIC To validate your work, run the following cell.
# MAGIC 
# MAGIC **Note:** This is just here to help keep you on track.

# COMMAND ----------

clearYourResults()

validateYourAnswer("01.A) DataFrame 2011", 1929623325, len(initDF_11.columns))
validateYourAnswer("01.B) DataFrame 2012", 1929623325, len(initDF_12.columns))
validateYourAnswer("01.C) DataFrame 2013", 1929623325, len(initDF_13.columns))
validateYourAnswer("01.D) DataFrame 2014", 1276280174, len(initDF_14.columns))
validateYourAnswer("01.E) DataFrame 2015", 1573909955, len(initDF_15.columns))
validateYourAnswer("01.F) DataFrame 2016", 1929623325, len(initDF_16.columns))
validateYourAnswer("01.G) DataFrame 2017", 1929623325, len(initDF_17.columns))
validateYourAnswer("01.H) DataFrame 2018", 1276280174, len(initDF_18.columns))

summarizeYourResults()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #2
# MAGIC 
# MAGIC **The next task is to load the schema for retailers and city:**
# MAGIC * Read in the parquet file **dbfs:/mnt/training/global-sales/cities/all.parquet/** and assign it to **citiesDF**. 
# MAGIC * Read in the parquet file **dbfs:/mnt/training/global-sales/retailers/all.parquet/** and assign it to **retailersDF**.
# MAGIC 
# MAGIC **But there's a problem with our data:**
# MAGIC * Take a look at the distinct list of countries.
# MAGIC * You should see that one country in particular is in there twice
# MAGIC   * Once fully spelled out
# MAGIC   * Once as an abreviation
# MAGIC   * **Hint:** It's the "land of the free and the home of the brave"
# MAGIC 
# MAGIC **Clean up the data:**
# MAGIC * Pick one of the two values (full name or abreviation)
# MAGIC * Update all the [incorrect] records to contain only one of the two values

# COMMAND ----------

# ANSWER
from pyspark import SparkContext
import pyspark

citiesDF = (spark.read.parquet("dbfs:/mnt/training/global-sales/cities/all.parquet/")
  .withColumn("country", when(col("country") == "USA", lit("United States")).otherwise(col("country")))
  .repartition(8) # Because I have 8 cores
)

retailersDF = (spark.read.parquet("dbfs:/mnt/training/global-sales/retailers/all.parquet/")
  .repartition(8) # Because I have 8 cores
)

# Cache and materialize...

cacheAs(citiesDF, "cities", pyspark.StorageLevel.MEMORY_ONLY).count()
cacheAs(retailersDF, "retailers", pyspark.StorageLevel.MEMORY_ONLY).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #2
# MAGIC 
# MAGIC To validate your work, run the following cell
# MAGIC 
# MAGIC **Note:** This is just here to help keep you on track.

# COMMAND ----------

clearYourResults()

validateYourAnswer("02.A) Cities Column Count", 135093849, len(citiesDF.columns))
validateYourAnswer("02.B) Cities Column", 646192812, "state" in citiesDF.columns)

validateYourAnswer("02.C) Retailers Column Count", 1929623325, len(retailersDF.columns))
validateYourAnswer("02.D) Retailers Column", 646192812, "retailer" in retailersDF.columns)

usaCount = max(citiesDF.filter(col("country") == "United States").count(), citiesDF.filter(col("country") == "USA").count())
validateYourAnswer("02.E) Expected 73883 US Cities", 1813573044, usaCount)

summarizeYourResults()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #3
# MAGIC 
# MAGIC It can be assumed that every dataset includes the "standard" columns:
# MAGIC * **transacted_at**
# MAGIC * **trx_id**
# MAGIC * **retailer_id**
# MAGIC * **description**
# MAGIC * **amount**
# MAGIC * **city_id**
# MAGIC 
# MAGIC However the datasets may vary from year to year.
# MAGIC 
# MAGIC For example:
# MAGIC * One dataset might include only the "standard" columns while another might add **year** and **month**. 
# MAGIC * One dataset might represent the **amount** as a string while another represents it as a decimal.
# MAGIC 
# MAGIC **Create a function to standardize each dataset:**
# MAGIC * Name the function **standardizeSchema**
# MAGIC * The function should take a single parameter of type **DataFrame**
# MAGIC * The function should return a **DataFrame**
# MAGIC * Drop any extra columns that might exist - (less data == faster processing)
# MAGIC * All 8 datasets should conform to the exact same schema
# MAGIC * Do not rename any of the columns loaded from Parquet
# MAGIC 
# MAGIC **Warning:** Make sure not to modify any DataFrame unnecissarily.  
# MAGIC This might cripple the Catalyst Optimizer, namely Predicate Pushdowns.  
# MAGIC This can be verified by examining the physical plan.

# COMMAND ----------

# ANSWER

def standardizeSchema(df):
  return df.select(col("transacted_at").cast("timestamp"), 
            col("trx_id").cast("integer"), 
            col("retailer_id").cast("integer"), 
            col("description").cast("string"), 
            col("amount").cast("decimal(38,2)"), 
            col("city_id").cast("integer"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #3
# MAGIC 
# MAGIC To validate your work, run the following cell
# MAGIC 
# MAGIC **Note:** This is just here to help keep you on track.

# COMMAND ----------

def validateTrxSchema(year, df):
  validateYourAnswer("03.{}.A) Has 6 columns".format(year), 1929623325, len(df.columns))
  
  schemaStr = str(df.schema)
  validateYourAnswer("03.{}.B) Contains transacted_at".format(year), 646192812, "transacted_at,TimestampType" in schemaStr)
  validateYourAnswer("03.{}.C) Contains trx_id".format(year), 646192812, "trx_id,IntegerType" in schemaStr)
  validateYourAnswer("03.{}.D) Contains retailer_id".format(year), 646192812, "retailer_id,IntegerType" in schemaStr)
  validateYourAnswer("03.{}.E) Contains description".format(year), 646192812, "description,StringType" in schemaStr)
  validateYourAnswer("03.{}.F) Contains amount".format(year), 646192812, "amount,DecimalType" in schemaStr)
  validateYourAnswer("03.{}.G) Contains city_id".format(year), 646192812, "city_id,IntegerType" in schemaStr)

# COMMAND ----------

clearYourResults()
validateTrxSchema(2011, standardizeSchema(initDF_11))
summarizeYourResults()

# COMMAND ----------

clearYourResults()
validateTrxSchema(2012, standardizeSchema(initDF_12))
summarizeYourResults()

# COMMAND ----------

clearYourResults()
validateTrxSchema(2013, standardizeSchema(initDF_13))
summarizeYourResults()

# COMMAND ----------

clearYourResults()
validateTrxSchema(2014, standardizeSchema(initDF_14))
summarizeYourResults()

# COMMAND ----------

clearYourResults()
validateTrxSchema(2015, standardizeSchema(initDF_15))
summarizeYourResults()

# COMMAND ----------

clearYourResults()
validateTrxSchema(2016, standardizeSchema(initDF_16))
summarizeYourResults()

# COMMAND ----------

clearYourResults()
validateTrxSchema(2017, standardizeSchema(initDF_17))
summarizeYourResults()

# COMMAND ----------

clearYourResults()
validateTrxSchema(2018, standardizeSchema(initDF_18))
summarizeYourResults()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #4
# MAGIC 
# MAGIC Our data exists in two lookup tables (retailer and city) and one additional table (transactions) per year.
# MAGIC 
# MAGIC That means if we want to pull in data such as the retailer name or the city and state, we have to do a join across 3 tables.
# MAGIC 
# MAGIC Joins of this type can be really expensive for consecutive queries.
# MAGIC 
# MAGIC To help optimize for future queries, we need to denormalization our data and then save that off for future use.
# MAGIC 
# MAGIC **Create a function to denormalize a dataset by joining retailers, cities and transaction:**
# MAGIC * Name the function **denormalize**
# MAGIC * The function should take a single parameter of type **DataFrame**
# MAGIC * The function should return a **DataFrame** which is a join of **initDF_all**, **retailersDF** and **citiesDF**
# MAGIC * The function should drop all the unnecissary columns: 
# MAGIC   * **city_id**
# MAGIC   * **retailer_id** 
# MAGIC   * **trx_id**
# MAGIC   * **us_sales**
# MAGIC   * **other_sales**
# MAGIC   * **all_sales**
# MAGIC   * **us_vs_world**
# MAGIC   
# MAGIC * **Hint #1:** Dropping the column at the right time can significantly incrase performance of the join.
# MAGIC * **Hint #2:** Consider the type of join being executed to futher increase performance.

# COMMAND ----------

# ANSWER

def denormalize(df):
  retailerNamesDF = retailersDF.drop("us_sales", "other_sales", "all_sales", "us_vs_world")
  df.drop("trx_id")
    .join(broadcast(citiesDF), "city_id")
    .join(retailerNamesDF, "retailer_id")
    .drop("city_id", "retailer_id")

# COMMAND ----------

# ANSWER

denormalizeDF_11 = denormalize(standardizeSchema(initDF_11))
denormalizeDF_12 = denormalize(standardizeSchema(initDF_12))
denormalizeDF_13 = denormalize(standardizeSchema(initDF_13))
denormalizeDF_14 = denormalize(standardizeSchema(initDF_14))
denormalizeDF_15 = denormalize(standardizeSchema(initDF_15))
denormalizeDF_16 = denormalize(standardizeSchema(initDF_16))
denormalizeDF_17 = denormalize(standardizeSchema(initDF_17))
denormalizeDF_18 = denormalize(standardizeSchema(initDF_18))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #4
# MAGIC 
# MAGIC To validate your work, run the following cell.
# MAGIC 
# MAGIC **Note:** This is just here to help keep you on track.

# COMMAND ----------

def validateDenormalize(year, df):
  validateYourAnswer(s"04.$year.A) Has 8 columns", 1276280174, df.columns.size)

  val schemaStr = str(df.schema)
  validateYourAnswer(s"04.$year.B) Contains transacted_at [Timestamp]", 646192812, "transacted_at,TimestampType" in schemaStr)
  validateYourAnswer(s"04.$year.C) Contains description [String]", 646192812, "description,StringType" in schemaStr)
  validateYourAnswer(s"04.$year.D) Contains amount [Decimal]", 646192812, "amount,DecimalType" in schemaStr)
  validateYourAnswer(s"04.$year.E) Contains city [String]", 646192812, "city,StringType" in schemaStr)
  validateYourAnswer(s"04.$year.F) Contains state [State]", 646192812, "state,StringType" in schemaStr)
  validateYourAnswer(s"04.$year.G) Contains state_abv [String]", 646192812, "state_abv,StringType" in schemaStr)
  validateYourAnswer(s"04.$year.H) Contains country [String]", 646192812, "country,StringType" in schemaStr)
  validateYourAnswer(s"04.$year.I) Contains retailer [String]", 646192812, "retailer,StringType" in schemaStr)

clearYourResults()

# COMMAND ----------

clearYourResults()
validateDenormalize(2011, denormalize(standardizeSchema(initDF_11)))
summarizeYourResults()

# COMMAND ----------

clearYourResults()
validateDenormalize(2012, denormalize(standardizeSchema(initDF_12)))
summarizeYourResults()

# COMMAND ----------

clearYourResults()
validateDenormalize(2013, denormalize(standardizeSchema(initDF_13)))
summarizeYourResults()

# COMMAND ----------

clearYourResults()
validateDenormalize(2014, denormalize(standardizeSchema(initDF_14)))
summarizeYourResults()

# COMMAND ----------

clearYourResults()
validateDenormalize(2015, denormalize(standardizeSchema(initDF_15)))
summarizeYourResults()

# COMMAND ----------

clearYourResults()
validateDenormalize(2016, denormalize(standardizeSchema(initDF_16)))
summarizeYourResults()

# COMMAND ----------

clearYourResults()
validateDenormalize(2017, denormalize(standardizeSchema(initDF_17)))
summarizeYourResults()

# COMMAND ----------

clearYourResults()
validateDenormalize(2018, denormalize(standardizeSchema(initDF_18)))
summarizeYourResults()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #5
# MAGIC 
# MAGIC One of our tasks is to write the entire dataset out to disk.
# MAGIC 
# MAGIC If we write the data out as-is, we perpetuate our skew, tiny files, and other problems.
# MAGIC 
# MAGIC **Repartition the dataframes so that each part-file, on disk, is ~100 MB each:**
# MAGIC * We'll call anything between 95 and 115 MBs good.
# MAGIC * There are at least three different strategies for solving this.
# MAGIC   * If you need a temp file, use **tempParquetPath** (declared below)
# MAGIC * You don't want the most accurate method, but the most effecient.
# MAGIC * Write the final parquet file to ** *USERHOME*/tuning/capstone-*YEAR*.parquet ** where
# MAGIC   * ** *USERHOME* ** is your home directory defined the variable **userhome**
# MAGIC   * ** *YEAR* ** is the year correspending to the set of transactions being processed.

# COMMAND ----------

path = "/user/dorothy.kucar@databricks.com/2014-fast.parquet"

bytes = sum([file.size for file in  dbutils.fs.ls(path) if file.name.endswith("parquet")])
print(bytes)

# COMMAND ----------

# ANSWER

def computePartitions(count, path):

  # Goal of 100 MB per part file
  maxParSize = 100 * 1000 * 1000

  # Count the number of bytes in our parquet file
  # Assumes that the dataset is not partitioned
  bytes = sum([file.size for file in  dbutils.fs.ls(path) if file.name.endswith("parquet")])
  
  # The actual size of our sample
  bytesPerRec = double(bytes) / double(count)

  recPerPar = maxParSize / bytesPerRec
  Math.round(count / recPerPar).toInt # partitions

# COMMAND ----------

repartitionPath = "/user/dorothy.kucar@databricks.com/2014-fast.parquet"
nonZeroBytes = [int(file.size/1000/1000) for file in  dbutils.fs.ls(repartitionPath) if int(file.size/1000/1000) > 0]
print("".join(str(nonZeroBytes)).rstrip("]").lstrip("["))

# COMMAND ----------

def repartitionAndWrite(repartitionPath, df, partitionTo):
  print("Repartitioning to {} partitions.".format(partitionTo))
  df.repartition(partitionTo).write.mode("overwrite").parquet(repartitionPath)

  print("File saved to {}.".format(repartitionPath))
  nonZeroBytes = [int(file.size/1000/1000) for file in  dbutils.fs.ls(repartitionPath) if int(file.size/1000/1000) > 0]
  print("".join(str(nonZeroBytes)).rstrip("]").lstrip("["))
  
  return repartitionPath

# COMMAND ----------

# ANSWER

def getTempPath(year):
  "{}/tuning/capstone-{}-temp.parquet".format(userhome, year)

def computeRepartitionAndWrite(year, df, tempPartitions):
  # tempPartitions: I know from experimentation that 8 is safe
  # but it is not the magic number of partitions for all datasets

  repartitionPath = "{}/tuning/capstone-{}.parquet".format(userhome, year)
  
  try:
    dbutils.fs.ls(repartitionPath)
    print("Expected file to NOT exist, skipping {}").format(repartitionPath)
    return repartitionPath
  except Exception:
    print("")
  
  tempPath = getTempPath(year)
  
  print("Reducing to {} partitions.").format(tempPartitions)
  df.repartition(tempPartitions).write.mode("overwrite").parquet(tempPath)
  
  print("File saved to {}.").format(tempPath)
  nonZeroBytes = [int(file.size/1000/1000) for file in  dbutils.fs.ls(tempPath) if int(file.size/1000/1000) > 0]
  print("".join(str(nonZeroBytes)).rstrip("]").lstrip("["))

  println("Counting records...")
  newDF = spark.read.parquet(tempPath)
  count = newDF.count()
  
  partitions = computePartitions(count, tempPath)

  repartitionAndWrite(repartitionPath, newDF, partitions)

# COMMAND ----------

# ANSWER

# ~6 minutes 
computeRepartitionAndWrite(2011, denormalizeDF_11)

# COMMAND ----------

# ANSWER

# ~5 minutes
computeRepartitionAndWrite(2012, denormalizeDF_12)

# COMMAND ----------

# ANSWER

# 1.8 hours
computeRepartitionAndWrite(2013, denormalizeDF_13)

# COMMAND ----------

# ANSWER

#5.5 minutes
computeRepartitionAndWrite(2014, denormalizeDF_14)

# COMMAND ----------

# ANSWER

# 5.5 minutes
computeRepartitionAndWrite(2015, denormalizeDF_15)

# COMMAND ----------

# ANSWER

# ?? hours
computeRepartitionAndWrite(2016, denormalizeDF_16)

# COMMAND ----------

# ANSWER

# 4.8 minutes
computeRepartitionAndWrite(2017, denormalizeDF_17)

# COMMAND ----------

# ANSWER

# 17.5 minutes
computeRepartitionAndWrite(2018, denormalizeDF_18)

# COMMAND ----------

# ANSWER

for i in range(2011, 2019):
  path = "dbfs:/user/jacob.parr@databricks.com/tuning/capstone-{}.parquet".format(i)
  nonZeroBytes = [int(file.size/1000/1000) for file in  dbutils.fs.ls(path) if int(file.size/1000/1000) > 0]
  print("{}: ".format(i) + "".join(str(nonZeroBytes)).rstrip("]").lstrip("["))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #5
# MAGIC 
# MAGIC To validate your work, run the following cell.
# MAGIC 
# MAGIC **Note:** This is just here to help keep you on track.

# COMMAND ----------

def validatePartitions(year):
  path = "{}/tuning/capstone-{}.parquet".format(userhome, year)
  sizes = [int(file.size/1000/1000) for file in  dbutils.fs.ls(path) if int(file.size/1000/1000) > 0]
  for i in range(0, len(sizes)):
    size = sizes[i]
    validateYourAnswer("05.{}.#{}) Between 95 & 115".format(year, i), 646192812, size >= 95 & size <= 115)
    print("05.{}.#{}) Actual size: {}".format(year, i, size))

# COMMAND ----------

clearYourResults()
validatePartitions(2011)

# COMMAND ----------

clearYourResults()
validatePartitions(2012)

# COMMAND ----------

clearYourResults()
validatePartitions(2013)

# COMMAND ----------

clearYourResults()
validatePartitions(2014)

# COMMAND ----------

clearYourResults()
validatePartitions(2015)

# COMMAND ----------

clearYourResults()
validatePartitions(2016)

# COMMAND ----------

clearYourResults()
validatePartitions(2017)

# COMMAND ----------

clearYourResults()
validatePartitions(2018)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #6A & #6B
# MAGIC 
# MAGIC At this point our on-disk issues with each year of data should be fixed.
# MAGIC 
# MAGIC We can effeciently execute queries on one dataset at a time.
# MAGIC 
# MAGIC We now need to prepare to execute queries on the entire dataset.
# MAGIC 
# MAGIC **Create a single `DataFrame` that consists of all 8 years (2011 to 2018):**
# MAGIC * Assign the final `DataFrame` to **dfAll**
# MAGIC * The solution should 
# MAGIC   * Keep to the previous guidelines, namely ~100 MB per part file
# MAGIC   * Lend itself to easily adding new datasets (e.g. 2019, 2020, etc)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Goal for Challenge #6A
# MAGIC * Execute a count of all records in under 20 seconds.
# MAGIC * One solution in particular is quicker to implement and still yields decent runtimes.

# COMMAND ----------

# ANSWER

#######################################################################################
# Solution for 6A 
# This is the solution for 6A which is simpy to union all the datasets together.
#######################################################################################
schema = "transacted_at timestamp, description string, amount decimal(38,2), city string, state string, state_abv string, country string, retailer string"

dfAll =    (spark.read.schema(schema).parquet("{}/tuning/capstone-2011.parquet").format(userhome)
  .unionByName(spark.read.schema(schema).parquet("{}/tuning/capstone-2012.parquet").format(userhome))
  .unionByName(spark.read.schema(schema).parquet("{}/tuning/capstone-2013.parquet").format(userhome))
  .unionByName(spark.read.schema(schema).parquet("{}/tuning/capstone-2014.parquet").format(userhome))
  .unionByName(spark.read.schema(schema).parquet("{}/tuning/capstone-2015.parquet").format(userhome))
  .unionByName(spark.read.schema(schema).parquet("{}/tuning/capstone-2016.parquet").format(userhome))
  .unionByName(spark.read.schema(schema).parquet("{}/tuning/capstone-2017.parquet").format(userhome))
  .unionByName(spark.read.schema(schema).parquet("{}/tuning/capstone-2018.parquet").format(userhome))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge Goal for 6B
# MAGIC * Execute a count of all records in under 2 seconds.
# MAGIC * The hardest of all the challenges, this solution takes a bit mroe work to setup.

# COMMAND ----------

# ANSWER

#######################################################################################
# Solution for 6B
# This is the solution for 6B which is to repartition and write everything to disk
#######################################################################################

path = computeRepartitionAndWrite(9999, dfAll, dfAll.rdd.getNumPartitions)

schema = "transacted_at timestamp, description string, amount decimal(38,2), city string, state string, state_abv string, country string, retailer string"
dfAll = spark.read.schema(schema).parquet(path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #6A & #6B
# MAGIC 
# MAGIC To validate your work, run the following cell.
# MAGIC 
# MAGIC **Note:** Benchmarking can be finiky. 
# MAGIC   * Occasionally (due to various uncontrollable circomstances) these queries can take up to 4x the average. 
# MAGIC   * Run the query 3-4 times to establish a average.

# COMMAND ----------

#results = tracker.track(() => {
#  dfAll.count

#print(f"Duration:      ${results.duration/1000.0}%,.1f seconds")
#print(f"Total Records: ${results.result}%,d")

# COMMAND ----------

clearYourResults()
validateYourAnswer("06) Final Record Count", 2074954664, results.result)
validateYourAnswer("06.A) Less than 20 seconds", 646192812, results.duration < 20*1000)
validateYourAnswer("06.B) Less than 2 seconds", 646192812, results.duration < 2*1000)
summarizeYourResults()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>