# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #Optimizing with Databricks Delta
# MAGIC 
# MAGIC **Dataset:**
# MAGIC * This is synthetic data generated specifically for these exercises
# MAGIC * Each year's data is roughly the same with some variation for market growth
# MAGIC * We are looking at retail purchases from the top N retailers
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC * Use Databricks Delta to solve the small files problem 

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cells to configure our "classroom", initialize our labs and pull in some utility methods:

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %run "../Includes/Initialize-Labs"

# COMMAND ----------

# MAGIC %run "../Includes/Utility-Methods"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Query on a Partitioned Dataset with Tiny Files
# MAGIC 
# MAGIC Here we do a query by `month`.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Load Data for 2018
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We don't have to specify a schema.

# COMMAND ----------

path2018 = "/mnt/training/global-sales/transactions/2018.parquet/"
trx2018DF = spark.read.parquet(path2018)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Notice below how there are many tiny files, namely in November.
# MAGIC 
# MAGIC This will induce a significant performance problem for us.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> It is also the reason it takes so long just to initialize the DataFrame in the previous cell.

# COMMAND ----------

for month in range(1,13):
  path = "{}/year=2018/month={}".format(path2018, month)
  count = len(dbutils.fs.ls(path))
  print("Month #{} count: {}".format(month, count))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Time Query for Partitioned 2018 Data
# MAGIC 
# MAGIC The utility function **`benchmarkCount()`** times the run of the count query.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> The query is very slow. You may wish to Cancel it after a minute or so.

# COMMAND ----------

(df, total, duration) = benchmarkCount(lambda : trx2018DF) 
print("Duration: {:,} ms".format(int(duration)))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Create a (Databricks Delta) Table
# MAGIC 
# MAGIC Let's see if we can improve the query speed by applying some Databricks Delta features.
# MAGIC 
# MAGIC * Write partitioned 2018 data to a Databricks Delta directory
# MAGIC * Create a table called `Transactions_2018_Delta` because Databricks Delta works with tables
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You can also convert an existing parquet dataset to Databricks Delta format! See <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/convert-to-delta.html" target="_blank">Convert to Databricks Delta</a>.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> This step takes a while.

# COMMAND ----------

basePathDelta2018 = userhome + "/delta-2018"

if ("delta-2018/" in list(map(lambda x: x.name, dbutils.fs.ls(userhome)))):
  print("The data already exists - no need to recreate it.")
else:
  
  (trx2018DF.write            # Get the DataFrameWriter
    .format("delta")          # Specify the format as "delta"
    .save(basePathDelta2018)  # And write it out to this path
  )

transactionTable2018 = "Transactions_2018_Delta"

# COMMAND ----------

spark.sql("""
  CREATE TABLE IF NOT EXISTS {}
  USING DELTA
  LOCATION '{}'
""".format(transactionTable2018, basePathDelta2018))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge small files into bigger files of around 1GB using OPTIMIZE
# MAGIC 
# MAGIC The query would become much faster if we collect all smaller files with related data into bigger files.
# MAGIC 
# MAGIC Databricks Delta has such a feature, called **`OPTIMIZE`**. 
# MAGIC 
# MAGIC More information is provided in the document on 
# MAGIC <a href="https://docs.databricks.com/delta/optimizations.html#id2" target="_blank">Databricks Delta Optimizations</a>.

# COMMAND ----------

spark.sql("OPTIMIZE {} ".format(transactionTable2018))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Query for Optimized 2018 Data
# MAGIC 
# MAGIC Using the utility function `benchmarkCount()`

# COMMAND ----------

delta2018DF = spark.read.table(transactionTable2018)

(df, total, duration) = benchmarkCount( lambda : delta2018DF)

print("Duration: {} ms".format(int(duration)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Queries on Partitioned vs Unpartitioned Data 
# MAGIC Here we do a query by `month`, which is a partition column in the 2014 dataset
# MAGIC * 2014 is partitioned by Year and Month (there is only one year!). So a query for November only looks at the `month=11` partition.
# MAGIC * 2017 is the same data, but it is not partitioned in any way. So a query for November has to look through the entire data set. 

# COMMAND ----------

path2014 = "/mnt/training/global-sales/transactions/2014.parquet"
trx2014DF = spark.read.parquet(path2014)

path2017 = "/mnt/training/global-sales/transactions/2017.parquet"
trx2017DF = spark.read.parquet(path2017)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Query for Partitioned 2014 Data
# MAGIC 
# MAGIC Here we show you how fast a simple query is when data is partitioned on query predicates.
# MAGIC 
# MAGIC * Query all rows with month=11
# MAGIC * This query should be fairly fast

# COMMAND ----------

parquet2014DF = trx2014DF.filter("month == 11")

(df, total, duration) = benchmarkCount( lambda : parquet2014DF) 

print("Duration: {} ms".format(int(duration)))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Time Query for Unpartitioned 2017 Data
# MAGIC 
# MAGIC This dataset is approximately the same size/quality as the 2014 set.
# MAGIC 
# MAGIC Extract `year` and `month` from `transacted_at` column.
# MAGIC 
# MAGIC * Query all rows with month=11
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> This query will take a while. Orders of magnitude more than the 2014 query. Feel free to cancel it after a minute or so.

# COMMAND ----------

from pyspark.sql.functions import month

parquet2017DF = (trx2017DF
  .withColumn("month", month("transacted_at"))
  .filter("month == 11")
)
(df, total, duration) = benchmarkCount( lambda : parquet2017DF) 
print("Duration: {:,} seconds".format(int(duration/1000)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform Unpartitioned 2017 Data to Databricks Delta
# MAGIC 
# MAGIC Use Databricks Delta's `ZORDER` feature to co-locate data that belongs to the same month.

# COMMAND ----------

basePathDelta2017 = userhome + "/delta-2017"

if ("delta-2017/" in list(map(lambda x: x.name, dbutils.fs.ls(userhome)))):
  print("The data already exists - no need to recreate it.")
else:
  
  (parquet2017DF.write        # Get the DataFrameWriter
    .format("delta")          # Specify the format as "delta"
    .save(basePathDelta2017)  # And write it out to this path
  )

transactionTable2017 = "Transactions_2017_Delta"

# COMMAND ----------

spark.sql("""
  CREATE TABLE IF NOT EXISTS {}
  USING DELTA
  LOCATION '{}' 
  """.format(transactionTable2017, basePathDelta2017))

# COMMAND ----------

spark.sql("OPTIMIZE {} ZORDER BY (month)".format(transactionTable2017))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Time Query for ZORDERED 2017 Data
# MAGIC You'll note the speed up puts it in the same order of magnitude as the partitioned 2014 dataset.
# MAGIC 
# MAGIC * Query all rows with month=11
# MAGIC * <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We are using **`ZORDER`** to mimic partitioning of data here

# COMMAND ----------

from pyspark.sql.functions import month
delta2017DF = (spark.read.table(transactionTable2017)
  .withColumn("month", month("transacted_at"))
  .filter("month == 11")
)

(df, total, duration) = benchmarkCount( lambda : delta2017DF )
print("Duration: {:,} ms".format(int(duration)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up

# COMMAND ----------

dbutils.fs.rm(basePathDelta2017, True)
dbutils.fs.rm(basePathDelta2018, True)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS {}".format(transactionTable2017))
spark.sql("DROP TABLE IF EXISTS {}".format(transactionTable2018))


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>