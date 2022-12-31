# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Data Cleansing Lab with Airbnb
# MAGIC 
# MAGIC <img src="http://insideairbnb.com/images/insideairbnb_graphic_site_1200px.png" style="width:800px"/>
# MAGIC 
# MAGIC The dataset we'll be working with is from Airbnb rentals in San Francisco.
# MAGIC 
# MAGIC You can find more information here:<br>
# MAGIC http://insideairbnb.com/get-the-data.html

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Reading the data

# COMMAND ----------

# MAGIC %md
# MAGIC We downloaded the <a href="http://insideairbnb.com/get-the-data.html" target="_blank">AirBNB San Francisco Listings</a> and put them to *dbfs* as is. Let's take a look:

# COMMAND ----------

# MAGIC %fs ls /mnt/training/airbnb/sf-listings/sf-listings.csv

# COMMAND ----------

# MAGIC %md
# MAGIC Examine the contents of `/mnt/training/airbnb/sf-listings/sf-listings.csv`.
# MAGIC 
# MAGIC Use `dbutils.fs` or `%fs`. You will find some usage examples in the <a href="https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html#access-dbfs-with-dbutils">Databricks Documentation</a> or you can invoke the `dbutils.fs.help()` to see how `dbutils.fs` works.

# COMMAND ----------

# ANSWER
print(dbutils.fs.head("mnt/training/airbnb/sf-listings/sf-listings.csv", 3000))

# COMMAND ----------

# MAGIC %md
# MAGIC Read the listings data into a DataFrame.
# MAGIC 
# MAGIC You will need to use some of the options available for the <a target="_blank" href="https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html#csv-scala.collection.Seq-">the Spark CSV Reader</a>. 
# MAGIC 
# MAGIC *Hint*: 
# MAGIC   * Check out the **Data Cleansing** Notebook for some useful options.
# MAGIC   * Check the *id* field of the record that contains *AirBedAndBreakfast* to get a hint on how to escape quotation marks.

# COMMAND ----------

# ANSWER
filePath = "mnt/training/airbnb/sf-listings/sf-listings.csv"
rawDF = (spark
         .read
         .option("header", "true")
         .option("multiLine", "true")
         .option("inferSchema", "true")
         .option("escape",'"')
         .csv(filePath))

display(rawDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Test your solution:

# COMMAND ----------

from pyspark.sql.types import *

cnt = rawDF.count()
assert cnt == 4804, "Number of records, expected " + str(4804) + " found " + str( cnt )
assert rawDF.schema[0].dataType == IntegerType(), "Column `id` excepted to be IntegerType, got " + str(rawDF.schema[0].dataType)
print("Tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC For the sake of simplicity, only keep certain columns from this dataset.

# COMMAND ----------

columnsToKeep = [
  "host_is_superhost",
  "cancellation_policy",
  "instant_bookable",
  "host_total_listings_count",
  "neighbourhood_cleansed",
  "zipcode",
  "latitude",
  "longitude",
  "property_type",
  "room_type",
  "accommodates",
  "bathrooms",
  "bedrooms",
  "beds",
  "bed_type",
  "minimum_nights",
  "number_of_reviews",
  "review_scores_rating",
  "review_scores_accuracy",
  "review_scores_cleanliness",
  "review_scores_checkin",
  "review_scores_communication",
  "review_scores_location",
  "review_scores_value",
  "price"]

baseDF = rawDF.select(columnsToKeep)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's cache it

# COMMAND ----------

baseDF.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC Well done! Now we have a DataFrame we can cleanse and then use for Machine Learning.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Data Cleansing

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fixing the data types

# COMMAND ----------

baseDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC If you take a look above, you will see that the `price` field got picked up as *string*. Let's see why:

# COMMAND ----------

display(baseDF.select("price"))

# COMMAND ----------

# MAGIC %md
# MAGIC In the next cell we will create a numeric *price* column:
# MAGIC  * Retain the original *price* column under the name *price_raw*
# MAGIC  * Remove the `$` and `,` characters from the *price_raw* column, cast it to the `Decimal(10,2)` type and name it back to *price*.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import *

fixedPriceDF = (baseDF
                .withColumnRenamed("price", "price_raw")
                .withColumn("price", regexp_replace(col("price_raw"), "[\$,]", "").cast("Decimal(10,2)")))

# COMMAND ----------

# MAGIC %md
# MAGIC Verify the results by displaying the *price* and the *price_raw* columns of a few records.

# COMMAND ----------

# ANSWER
display(fixedPriceDF.select("price_raw", "price"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Getting rid of *nulls*
# MAGIC 
# MAGIC Use the *describe* DataFrame function to see whether there are *nulls* in `fixedPriceDF`.

# COMMAND ----------

# ANSWER
display(fixedPriceDF.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ** 1. Cleansing Categorical features**
# MAGIC 
# MAGIC There are a few nulls in the categorical feature `zipcode` and `host_is_superhost`. Let's get rid of those rows where any of these columns is null.

# COMMAND ----------

# ANSWER
noNullsDF = fixedPriceDF.na.drop(subset=["zipcode", "host_is_superhost"])

# COMMAND ----------

# MAGIC %md
# MAGIC Test your solution:

# COMMAND ----------

cnt = noNullsDF.count()
assert cnt == 4746, "Number of records, expected " + str(4746) + " found " + str( cnt )
print("Tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ** 2. Imputing Nominal Features **
# MAGIC 
# MAGIC For each numeric column, replace the *null* values with the median of the non-null values in that column.
# MAGIC 
# MAGIC We will use the `Imputer` Spark ML module for this. The `Imputer` favors `Double` type features. So firts, let's change the type of our nominal columns to `Double`.

# COMMAND ----------

from pyspark.sql.types import *

integerColumns = [x.name for x in baseDF.schema.fields if x.dataType == IntegerType()]
doublesDF = noNullsDF

for c in integerColumns:
  doublesDF = doublesDF.withColumn(c, col(c).cast("double"))

print("Columns converted from Integer to Double:\n - {}".format("\n - ".join(integerColumns)))

# COMMAND ----------

doublesDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Now create an *Imputer*, set its *strategy* to median and impute the columns that contain null values. Keep the name of the *output Columns* the same as the *input Columns*.
# MAGIC 
# MAGIC First, add all the numeric columns that contain *null* values to the `imputeCols` array. 

# COMMAND ----------

# ANSWER
from pyspark.ml.feature import Imputer

imputeCols = [
              "host_total_listings_count",
              "bathrooms",
              "beds", 
              "review_scores_rating",
              "review_scores_accuracy",
              "review_scores_cleanliness",
              "review_scores_checkin",
              "review_scores_communication",
              "review_scores_location",
              "review_scores_value"]

imputer = Imputer()
imputer.setStrategy("median")
imputer.setInputCols(imputeCols)
imputer.setOutputCols(imputeCols)

imputedDF = imputer.fit(doublesDF).transform(doublesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Test your solution:

# COMMAND ----------

impCnt = imputedDF.count()
noNullsCnt = imputedDF.na.drop().count()
assert impCnt == noNullsCnt, "Found null values in imputedDF."

print("Tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Getting rid of extreme values
# MAGIC 
# MAGIC Let's take a look at the *min* and *max* values of the `price` column:
# MAGIC 
# MAGIC *Hint*: Use the `describe` function

# COMMAND ----------

# ANSWER
display(imputedDF.select("price").describe())

# COMMAND ----------

# MAGIC %md
# MAGIC %md There are some super-expensive listings. But that's the Data Scientist's job to decide what to do with them. We can certainly filter the "free" AirBNBs though.
# MAGIC 
# MAGIC Let's see first how many listings we can find where the *price* is zero.

# COMMAND ----------

# ANSWER
imputedDF.filter(col("price") == 0).count()

# COMMAND ----------

# MAGIC %md
# MAGIC Now only keep rows with a positive *price*.

# COMMAND ----------

# ANSWER

posPricesDF = imputedDF.filter(col("price") > 0)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at the *min* and *max* values of the *minimum_nights* column:

# COMMAND ----------

display(posPricesDF.select("minimum_nights").describe())

# COMMAND ----------

# MAGIC %md
# MAGIC A minimum of 100 million nights to stay? There are certainly some extremes here. Group the rows by *minimum_nights*, order by the `count` column descendent and display it on a barchart:

# COMMAND ----------

# ANSWER
display(posPricesDF.groupBy("minimum_nights").count().orderBy(col("count").desc(), col("minimum_nights")))

# COMMAND ----------

# MAGIC %md
# MAGIC A minimum stay of one year seems to be a reasonable limit here. Let's filter out those records where the *minimum_nights* is greater then 365:

# COMMAND ----------

# ANSWER
cleanDF = posPricesDF.filter(col("minimum_nights") <= 365)

# COMMAND ----------

display(cleanDF)

# COMMAND ----------

# MAGIC %md
# MAGIC OK, our data is cleansed now. Let's save this DataFrame to a file so that our Data Scientist friend can pick it up.

# COMMAND ----------

outputPath = userhome + "/airbnb-cleansed.parquet"
# Remove output folder if it exists
dbutils.fs.rm(outputPath, True)
cleanDF.write.parquet(outputPath);

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>