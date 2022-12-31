// Databricks notebook source
// MAGIC %md
// MAGIC # SparkSession 
// MAGIC 
// MAGIC In Spark 1.x it was nonintuitive to have HiveContext as an entry point to use the DataFrame API. Spark 2.0 introduced SparkSession, an entry point that subsumed SQLContext and HiveContext, though for backward compatibility, the two are preserved. SparkSession has many features and here we demonstrate some of the more important ones.
// MAGIC 
// MAGIC While this notebook is written in Scala, similar APIs exist in Python and Java.
// MAGIC 
// MAGIC To read the companion blog post, see [Apache Spark 2.0 Technical Preview](https://databricks.com/blog/2016/05/11/spark-2-0-technical-preview-easier-faster-and-smarter.html).

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creating a SparkSession
// MAGIC 
// MAGIC A SparkSession can be created using a builder pattern. The builder automatically reuse an existing SparkContext if one exists and creates a SparkContext if it does not exist. Configuration options set in the builder are automatically propagated to Spark and Hadoop during I/O.

// COMMAND ----------

// A SparkSession can be created using a builder pattern
import org.apache.spark.sql.SparkSession
val sparkSession = SparkSession.builder
  .master("local")
  .appName("my-spark-app")
  .config("spark.some.config.option", "config-value")
  .getOrCreate()

// COMMAND ----------

// MAGIC %md
// MAGIC In Databricks notebooks and Spark REPL, the SparkSession has been created automatically and assigned to variable `spark`.

// COMMAND ----------

spark

// COMMAND ----------

// MAGIC %md
// MAGIC ### Unified entry point for reading data
// MAGIC 
// MAGIC SparkSession is the entry point for reading data, similar to the old SQLContext.read.

// COMMAND ----------

val jsonData = spark.read.json("/home/webinar/person.json")

// COMMAND ----------

display(jsonData)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Running SQL queries
// MAGIC 
// MAGIC SparkSession can be used to execute SQL queries over data, getting the results back as a DataFrame (i.e. Dataset[Row]).

// COMMAND ----------

display(spark.sql("select * from person"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Working with config options
// MAGIC 
// MAGIC SparkSession can also be used to set runtime configuration options, which can toggle optimizer behavior or I/O (i.e. Hadoop) behavior.

// COMMAND ----------

spark.conf.set("spark.some.config", "abcd")

// COMMAND ----------

spark.conf.get("spark.some.config")

// COMMAND ----------

// MAGIC %md
// MAGIC And config options set can also be used in SQL using variable substitution.

// COMMAND ----------

// MAGIC %sql select "${spark.some.config}"

// COMMAND ----------

// MAGIC %md
// MAGIC ### Working with metadata directly
// MAGIC 
// MAGIC SparkSession also includes a `catalog` method that contains methods to work with the metastore (i.e. data catalog). Methods there return Datasets so you can use the same Dataset API to play with them.

// COMMAND ----------

// To get a list of tables in the current database
val tables = spark.catalog.listTables()

// COMMAND ----------

display(tables)

// COMMAND ----------

// Use the Dataset API to filter on names
display(tables.filter(_.name contains "son"))

// COMMAND ----------

// Get the list of columns for a table
display(spark.catalog.listColumns("smart"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Access to the underlying SparkContext
// MAGIC 
// MAGIC SparkSession.sparkContext returns the underlying SparkContext, used for creating RDDs as well as managing cluster resources.

// COMMAND ----------

spark.sparkContext