# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Cleansing with Apache Spark
# MAGIC 
# MAGIC In this lab we are covering some of the data cleansing techniques you can use in Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Reading the Data

# COMMAND ----------

filePath = "/mnt/training/data-cleansing/bookreviews-5.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at our CSV:

# COMMAND ----------

print(dbutils.fs.head(filePath))

# COMMAND ----------

# MAGIC %md
# MAGIC How will Spark parse this with the default CSV Reader?

# COMMAND ----------

rawDF = (spark
         .read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(filePath))

# COMMAND ----------

rawDF.printSchema()

# COMMAND ----------

display(rawDF)

# COMMAND ----------

# MAGIC %md
# MAGIC We have multiline records. Let's fix this by tuning <a target="_blank" href="https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html#csv-scala.collection.Seq-">the Spark CSV Reader</a>.

# COMMAND ----------

rawDF = (spark
         .read
         .option("header", "true")
         .option("multiLine", "true")
         .option("inferSchema", "true")
         .csv(filePath))

# COMMAND ----------

display(rawDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Almost perfect. Let's indicate that quotation marks are escaped with `"`:

# COMMAND ----------

rawDF = (spark
         .read
         .option("header", "true")
         .option("multiLine", "true")
         .option("inferSchema", "true")
         .option("escape",'"')
         .csv(filePath))

# COMMAND ----------

rawDF.printSchema()

# COMMAND ----------

display(rawDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Looks good!

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Data Cleansing Techniques

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Regular Expressions
# MAGIC 
# MAGIC We saw that the price is displayed in the `$xxx,xxx.xx` format. Let's remove the `$`s and `,`s and convert the price column to a numeric format. 

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

priceDF = rawDF.withColumn("price", regexp_replace(col("price"), "[\$,]", "").cast("Double"))
display(priceDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dealing with nulls

# COMMAND ----------

# MAGIC %md
# MAGIC #### Looking for nulls in a DataFrame
# MAGIC 
# MAGIC The `count` row of the DataFrame you get as a result of `df.describe()` will tell you how many non-null records are there in each column. Just compare these with the result of `df.count()` to see how many nulls there are. 
# MAGIC 
# MAGIC *We are also caching this DataFrame to speed up computation*.

# COMMAND ----------

priceDF.cache().count()

# COMMAND ----------

display(priceDF.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filtering corrupt rows

# COMMAND ----------

display(priceDF)

# COMMAND ----------

noCorruptRowsDF = priceDF.na.drop(subset=["id"])

display(noCorruptRowsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Imputing missing values.

# COMMAND ----------

# MAGIC %md
# MAGIC Now go and take a look at Spark ML *Imputer*.

# COMMAND ----------

from pyspark.ml.feature import Imputer

imputer = Imputer()
print(imputer.explainParams())

# COMMAND ----------

# MAGIC %md
# MAGIC Set up the *Imputer* on the `reviewsAVG` column and replace missing data with the mean of the existing values.

# COMMAND ----------

imputeCols = ["reviewsAvg"]
imputer.setInputCols(imputeCols)
imputer.setOutputCols(imputeCols)

imputedDF = imputer.fit(noCorruptRowsDF).transform(noCorruptRowsDF)

display(imputedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Replacing nulls with a custom value

# COMMAND ----------

cleanDF = imputedDF.fillna("<<MISSING REVIEWS>>","topReview")

# COMMAND ----------

# MAGIC %md
# MAGIC Now our DataFrame is cleansed. Good Job!

# COMMAND ----------

display(cleanDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>