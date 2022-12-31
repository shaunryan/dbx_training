# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #Comparing 2014 to 2017
# MAGIC 
# MAGIC **Dataset:**
# MAGIC * This is synthetic data generated specifically for these exercises
# MAGIC * Each year's data is roughly the same with some variation for market growth
# MAGIC * We are looking at retail purchases from the top N retailers
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC * Shorten development time
# MAGIC * Explore ways to increase query performance

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cells to configure our "classroom", initialize our labs and pull in some utility methods:

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %run "../Includes/Initialize Labs"

# COMMAND ----------

# MAGIC %run "../Includes/Utility Methods"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Introduction
# MAGIC 
# MAGIC We are going to start by looking at data for 2014.
# MAGIC 
# MAGIC We are then going to take a look at data for 2017.
# MAGIC 
# MAGIC Then we are then going to attempt to compare the total volume of sales for these two years.
# MAGIC 
# MAGIC Let's start with the basic setup:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data for 2014

# COMMAND ----------

from pyspark.sql.functions import *

path2014 = "/mnt/training/global-sales/transactions/2014.parquet"
trx2014DF = spark.read.parquet(path2014)

display(
  trx2014DF.limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data for 2017

# COMMAND ----------

path2017 = "/mnt/training/global-sales/transactions/2017.parquet"
trx2017DF = spark.read.parquet(path2017)

display(
  trx2017DF.limit(5)
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Compare the total number of records
# MAGIC 
# MAGIC We want to know programatically how long the query took so we are<br/>
# MAGIC going to employ a crude benchmark from our **Utility Methods** notebook.
# MAGIC 
# MAGIC We will introduce a better solution in a later module.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Run the next two cells and proceede to **While we are waiting... #1**

# COMMAND ----------

logPath = "{}/test-results".format(userhome)

# COMMAND ----------

(df, total2014Slow, duration14Slow) = benchmarkCount(lambda : trx2014DF)

# Save our numbers for later
logYourTest(logPath, "Total 2014-Slow", total2014Slow)
logYourTest(logPath, "Duration 2014-Slow", duration14Slow)

print("2014 Totals:    {:,} transactions".format(total2014Slow))
print("2014 Duration:  {:.3f} sec".format(duration14Slow/1000.0))

# COMMAND ----------

(df, total2017Slow, duration17Slow) = benchmarkCount(lambda : trx2017DF)

# Save our numbers for later
logYourTest(logPath, "Total 2017-Slow", total2017Slow)
logYourTest(logPath, "Duration 2017-Slow", duration17Slow)

print("2017 Totals:    {:,} transactions".format(total2017Slow))
print("2017 Duration:  {:.3f} sec".format(duration17Slow/1000.0))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC And if you are currious about the results we just logged,<br/>
# MAGIC you can view them by running the following command.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We will use these numbers later as we evaluate the results of different experiements.

# COMMAND ----------

df = loadYourTestResults(logPath)
display(df)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%">
# MAGIC   <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/> While we are waiting... #1</h2>
# MAGIC 
# MAGIC 
# MAGIC   <p>The count operation is going to take a "long" time. </p>
# MAGIC     
# MAGIC   <p>10-15 minutes just for the count.</p>
# MAGIC 
# MAGIC   <p>And it's only going to get longer as we start doing more with this data.</p>
# MAGIC 
# MAGIC   <p>How hard would it be to increase developer productivity?</p>
# MAGIC   
# MAGIC   <p>We can always run against the full dataset when we are done.</p>
# MAGIC 
# MAGIC   <p>Again, our immeadiate goal is to speed up our development.</p>
# MAGIC 
# MAGIC   <h3 style="margin-top:1em">What options do we have for making this run faster [without caching]?</h3>
# MAGIC   
# MAGIC   <ul>
# MAGIC     <li>Option #1...</li>
# MAGIC     <li>Option #2...</li>
# MAGIC     <li>Option #3...</li>
# MAGIC     <li>Option #4...</li>
# MAGIC     <li>Option #5...</li>
# MAGIC     <li>Option #6...</li>
# MAGIC     <li>Option #7...</li>
# MAGIC   </ul>
# MAGIC 
# MAGIC   <p><img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Take a look at the <b>Typed transformations</b> and the <b>Untyped transformations</b> section of the <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset" target="_blank">Scala docs for the class <b>Dataset</b></a> (aka DataFrame).</p>
# MAGIC   
# MAGIC   <p><img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Then take a look at the <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.SparkSession" target="_blank">Scala docs for the class <b>SparkSession</b></a>.</p>
# MAGIC 
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We need the results of the previous queries later so make sure the complete, eventually.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #1
# MAGIC 
# MAGIC Compared to the **count()** executed above, our next set of queries could take "forever."
# MAGIC 
# MAGIC 5-30 minutes for every query means we can be here all day.
# MAGIC 
# MAGIC And we don't get paid by the hour
# MAGIC 
# MAGIC **Implement our solution for speeding up our queries:**
# MAGIC * Remeber our goal: faster development by getting more iterations on our ETL
# MAGIC * Start with the DataFrames **trx2014DF** and **trx2017DF** respectively
# MAGIC * Take a **10% sample** of the data **without replacement**
# MAGIC   * For the sample seed, use **42** <a href="https://en.wikipedia.org/wiki/Phrases_from_The_Hitchhiker%27s_Guide_to_the_Galaxy#Answer_to_the_Ultimate_Question_of_Life,_the_Universe,_and_Everything_(42)" target="_blank">because...</a>
# MAGIC * Write the data to the temp files **fastPath14** and **fastPath17** respectively
# MAGIC * Read the "faster" data back in and assign the `DataFrame` to **fast2014DF** and **fast2017DF** respectively

# COMMAND ----------

# TODO

fastPath14 = "{}/2014-fast.parquet".format(userhome)
fastPath17 = "{}/2017-fast.parquet".format(userhome)

trx2014DF.FILL_IN    # write out a sample of the data to disk
trx2017DF.FILL_IN    # write out a sample of the data to disk

fast2014DF = FILL_IN # read in the faster parquet file
fast2017DF = FILL_IN # read in the faster parquet file

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #1
# MAGIC 
# MAGIC We can call your solution good if we can get it down to 1/10 of the original time or 9x faster.
# MAGIC 
# MAGIC **Note**: A 300x improvement is possible even with the full dataset
# MAGIC 
# MAGIC Run the cell below to validate your answer:

# COMMAND ----------

(df, total2014Fast, duration14Fast) = benchmarkCount(lambda: fast2014DF)
times14 = (duration14Slow - duration14Fast) / duration14Fast

(df, total2017Fast, duration17Fast) = benchmarkCount(lambda: fast2017DF)
times17 = (duration17Slow-duration17Fast)/duration17Fast


# Record our results for later
logYourTest(logPath, "Total 2014-Fast", total2014Fast)
logYourTest(logPath, "Duration 2014-Fast", duration14Fast)

logYourTest(logPath, "Total 2017-Fast", total2017Fast)
logYourTest(logPath, "Duration 2017-Fast", duration17Fast)


clearYourResults()
validateYourAnswer("01.A) Count 2014 Speed", 646192812, times14 > 9)
validateYourAnswer("01.B) Count 2017 Speed", 646192812, times17 > 9)
summarizeYourResults()

# COMMAND ----------

results = loadYourTestMap(logPath)
# results = loadYourTestMap("dbfs:/mnt/training-rw/global-sales/solutions/sample-results")

displayHTML("""
<html><style>body {{font-size:larger}} td {{padding-right:1em}} td.value {{text-align:right; font-weight:bold}}</style><body><table>
  <tr><td>2014 Duration A:</td><td class="value">{:,.2f} sec</td></tr>
  <tr><td>2014 Duration B:</td><td class="value">{:,.2f} sec</td></tr>
  <tr><td>Improvement:</td>    <td class="value">{:,d}x faster</td></tr>
  <tr><td>&nbsp;</td></tr>
  <tr><td>2017 Duration A:</td><td class="value">{:,.2f} sec</td></tr>
  <tr><td>2017 Duration B:</td><td class="value">{:,.2f} sec</td></tr>
  <tr><td>Improvement:</td>    <td class="value">{:,d}x faster</td></tr>
</table></html></body>""".format(results["Duration 2014-Slow"]/1000.0,
                                 results["Duration 2014-Fast"]/1000.0,
                                 int(times14),
                                 results["Duration 2017-Slow"]/1000.0,
                                 results["Duration 2017-Fast"]/1000.0,
                                 int(times17)))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%">
# MAGIC   <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/> While we are waiting... #2</h2>
# MAGIC 
# MAGIC   <p><b>What is wrong with our benchmarking strategy?</b></p>
# MAGIC 
# MAGIC   <p><b>Or rather what are some of the pitfalls in benchmarking?</b></p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #2
# MAGIC 
# MAGIC **Compare the amount of sales for each month:**
# MAGIC * For 2017 only
# MAGIC   * Add the column **year** by extracting it from **transacted_at**
# MAGIC   * Add the column **month** by extracting it from **transacted_at**
# MAGIC   * In 2014 someone had the presence of mind to do this for us
# MAGIC * Aggregate by **year** and then by **month**
# MAGIC * For each aggregate, sum the **amount**
# MAGIC * Sort the data by **year** and then by **month**
# MAGIC * Rename the aggregate value to **amount**
# MAGIC * The final schema must include:
# MAGIC   * `year`: `integer`
# MAGIC   * `month`: `integer`
# MAGIC   * `amount`: `decimal`
# MAGIC * For the 2014 dataset, assign the final `DataFrame` to **byMonth2014DF**
# MAGIC * For the 2017 dataset, assign the final `DataFrame` to **byMonth2017DF**

# COMMAND ----------

# TODO

byMonth2014DF = (fast2014DF
  no need to extract the year from transacted_at
  no need to extract the month from transacted_at
 .FILL_IN  # aggregate by year and month
 .FILL_IN  # sum up all transactions
 .FILL_IN  # rename the aggregate value 
 .FILL_IN  # sort the data by year and then month
)

display(byMonth2014DF.select("month", "amount"))

# COMMAND ----------

# TODO

byMonth2017DF = (fast2017DF
 .FILL_IN  # extract the year from transacted_at
 .FILL_IN  # extract the month from transacted_at
 .FILL_IN  # aggregate by year and month
 .FILL_IN  # sum up all transactions
 .FILL_IN  # rename the aggregate value 
 .FILL_IN  # sort the data by year and then month
)

display(byMonth2017DF.select("month", "amount"))

# COMMAND ----------

# MAGIC %md
# MAGIC When you are done, render the results above as a line graph.

# COMMAND ----------

# MAGIC %md
# MAGIC When you are done, render the results above as a line graph.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #2
# MAGIC Run the cells below to validate your answer:

# COMMAND ----------

clearYourResults()

validateYourSchema("02.A) byMonth2014DF", byMonth2014DF, "year", "integer")
validateYourSchema("02.B) byMonth2014DF", byMonth2014DF, "month", "integer")
validateYourSchema("02.C) byMonth2014DF", byMonth2014DF, "amount", "decimal")

count2014 = byMonth2014DF.count()
validateYourAnswer("02.D) 2014 expected 12 Records", 155192030, count2014)

summarizeYourResults()

# COMMAND ----------

clearYourResults()

validateYourSchema("02.E) byMonth2017DF", byMonth2017DF, "year", "integer")
validateYourSchema("02.F) byMonth2017DF", byMonth2017DF, "month", "integer")
validateYourSchema("02.G) byMonth2017DF", byMonth2017DF, "amount", "decimal")

count2017 = byMonth2017DF.count()
validateYourAnswer("02.H) 2017 expected 12 Records", 155192030, count2017)

summarizeYourResults()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> The following assertions will only pass if using the "faster" datasets and the exact same solution:  
# MAGIC Take a 10% sample of the data without replacement using the seed "42"

# COMMAND ----------

clearYourResults()

sum2014 = byMonth2014DF.select( sum("amount").cast("decimal(20,2)").cast("string").alias("total") ).first()[0]
validateYourAnswer("02.I) 2014 Sum", 1392416866, sum2014)

summarizeYourResults()

# COMMAND ----------

clearYourResults()

sum2017 = byMonth2017DF.select( sum("amount").cast("decimal(20,2)").cast("string").alias("total") ).first()[0]
validateYourAnswer("02.J) 2017 Sum", 1111158808, sum2017)

summarizeYourResults()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next, put our datasets to work
# MAGIC 
# MAGIC Let's merge the two results, present them as a line graph, and then compare.
# MAGIC 
# MAGIC You will need to adjust the plot options to group by year (Series groupings).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> There is a big difference between **union** and **unionByName**

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Review Challenges #1 & #2

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%">
# MAGIC   <h3>What can we conclude about the data?</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%">
# MAGIC   <h3>What are some of the ramifications/merits of our other options?</h3>
# MAGIC   <li>An Empty DataFrame</li>
# MAGIC   <li>Caching</li>
# MAGIC   <li>DataFrame.limit(n)</li>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) The Interesting Parts
# MAGIC 
# MAGIC We can make some rather bland conclusions about January to October
# MAGIC 
# MAGIC Something changes in November
# MAGIC 
# MAGIC And things really pick up in December.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's take a look at November and December
# MAGIC 
# MAGIC We are going to start over and zero in on the Christmas shopping season.
# MAGIC 
# MAGIC Instead of looking at a month at a time, let's break it down by day.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #3
# MAGIC 
# MAGIC **Compare the amount of sales in November & December of 2014 to 2017:**
# MAGIC * Start with our "faster" DataFrames **fast2017DF** and **fast2014DF** respectively
# MAGIC   * This will save time as you code and test the various transformations
# MAGIC   * **When you are done**, replace with **trx2014DF** and **trx2017DF** respectively
# MAGIC * For 2017 only
# MAGIC   * Add the column **month** by extracting it from **transacted_at**
# MAGIC   * Add the column **year** by extracting it from **transacted_at**
# MAGIC   * In 2014 someone had the presence of mind to do this for us
# MAGIC * Limit the datasets to November and December only
# MAGIC * For 2017 & 2014, add the column **day** by extracting it from **transacted_at**
# MAGIC   * Because the intent is to compare day-to-day over several months, use **dayofyear(..)**
# MAGIC * Aggregate by **year** and then by **day**
# MAGIC * For each aggregate, sum the **amount**
# MAGIC * Rename the aggregate value to **amount**
# MAGIC * Sort the data by **day**
# MAGIC * The final schema must include:
# MAGIC   * `year`: `integer`
# MAGIC   * `day`: `integer`
# MAGIC   * `amount`: `decimal`
# MAGIC * Do this for the 2017 and 2014 datasets
# MAGIC * Assign the final `DataFrame` to **holiday14DF** and **holiday17DF** respectively

# COMMAND ----------

# TODO

holiday14DF = (fast2014DF // trx2014DF
  # no need to extract the month from transacted_at
  # no need to extract the year from transacted_at
  .FILL_IN  // limit the data to Nov & Dec
  .FILL_IN  // extract the day from transacted_at
  .FILL_IN  // aggregate by year and day
  .FILL_IN  // sum up all transactions
  .FILL_IN  // rename the aggregate value 
  .FILL_IN  // sort day
)
# Don't use on the full dataset
# display(holiday14DF)

# COMMAND ----------

# TODO

holiday17DF = (fast2017DF // trx2017DF
  .FILL_IN  // extract the month from transacted_at
  .FILL_IN  // extract the year from transacted_at
  .FILL_IN  // limit the data to Nov & Dec
  .FILL_IN  // extract the day from transacted_at
  .FILL_IN  // aggregate by year and day
  .FILL_IN  // sum up all transactions
  .FILL_IN  // rename the aggregate value 
  .FILL_IN  // sort day
)
# Don't use on the full dataset
# display(holiday17DF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #3
# MAGIC Run the cells below to validate your answer.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This first set of validations will work with either the "fast" or "slow" datasets

# COMMAND ----------

# These should pass on the "slow" or "fast" datasets
clearYourResults()

validateYourSchema("03.A) holiday14DF", holiday14DF, "year", "integer")
validateYourSchema("03.B) holiday14DF", holiday14DF, "day", "integer")
validateYourSchema("03.C) holiday14DF", holiday14DF, "amount", "decimal")

validateYourSchema("03.D) holiday17DF", holiday17DF, "year", "integer")
validateYourSchema("03.E) holiday17DF", holiday17DF, "day", "integer")
validateYourSchema("03.F) holiday17DF", holiday17DF, "amount", "decimal")

summarizeYourResults()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This second set of validations will only work with the "slow" datasets.

# COMMAND ----------

# No cheating! :-)
spark.catalog.clearCache() 

# COMMAND ----------

clearYourResults()

########################################################
# Validate the 2014 count
(validated14, holidayCount14, holidayDuration14) = benchmarkCount(lambda:
  holiday14DF.withColumn("temp", lit(2014)).persist() 
)
validateYourAnswer("03.G 2014 expected 61 records", 1551101942, holidayCount14)

########################################################
# Validate the 2014 sum to ensure we have the right records
sum2014 = validated14.select( sum("amount").cast("decimal(20,2)").cast("string").alias("total") ).first()[0]
validateYourAnswer("03.H 2014 sum", 1358795957, sum2014)

########################################################

logYourTest(logPath, "Duration 2014-Holiday-Slow", holidayDuration14)

summarizeYourResults()

# COMMAND ----------

clearYourResults()

########################################################
# Validate the 2017 count
(validated17, holidayCount17, holidayDuration17) = benchmarkCount(lambda:
  holiday17DF.withColumn("temp", lit(2017)).persist() 
)
validateYourAnswer("03.I 2017 expected 61 records", 1551101942, holidayCount17)

########################################################
# Validate the 2017 sum to ensure we have the right records
sum2017 = validated17.select( sum("amount").cast("decimal(20,2)").cast("string").alias("total") ).first()[0]
validateYourAnswer("03.J 2017 sum", 760268806, sum2017)

########################################################

logYourTest(logPath, "Duration 2017-Holiday-Slow", holidayDuration17)

summarizeYourResults()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now we can merge the two and graph them side by side

# COMMAND ----------

bothHolidaysDF = validated14.unionByName(validated17)
display(bothHolidaysDF.select("day", "amount", "year"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%">
# MAGIC   <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/> Review Challenge #3, Part 1/3</h2>
# MAGIC   
# MAGIC   <h3>What can we conclude about the data?</h3>
# MAGIC   <h3>What are the spikes?</h3>
# MAGIC   <h3>Why did the level change between day 305 & 365?</h3>
# MAGIC   <h3>Why are the spikes not aligned?</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="display:table; height:600px; width:100%">
# MAGIC   <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/> Review Challenge #3, Part 2/3</h2>
# MAGIC   
# MAGIC   <h3>About how much time did we save by using the "faster" dataset?</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; width:100%">
# MAGIC   <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"> Review Challenge #3, Part 3/3</h2>
# MAGIC   <p>Start by reviewing the numbers from the benchmark of our initial counts:</p>
# MAGIC </div>

# COMMAND ----------

results = loadYourTestMap(logPath)
# results = loadYourTestMap("dbfs:/mnt/training-rw/global-sales/solutions/sample-results")

print("2014 Totals: {:,} transactions".format(int(results["Total 2014-Slow"])))
print("2017 Totals: {:,} transactions".format(int(results["Total 2017-Slow"])))
print()
print("2014 Count Duration:   {:,.3f} sec".format(results["Duration 2014-Slow"]/1000.0))
print("2017 Count Duration:   {:,.3f} sec".format(results["Duration 2017-Slow"]/1000.0))
print("Slow 2017 vs 2014:     {:,.2f}x faster".format((results["Duration 2014-Slow"] - results["Duration 2017-Slow"]) / results["Duration 2017-Slow"]))
print()
print("2014 Holiday Duration: {:,.3f} sec".format(results["Duration 2014-Holiday-Slow"]/1000.0))
print("2017 Holiday Duration: {:,.3f} sec".format(results["Duration 2017-Holiday-Slow"]/1000.0))
print("Holiday 2014 vs 2017:  {:,.2f}x faster".format((results["Duration 2017-Holiday-Slow"] - results["Duration 2014-Holiday-Slow"]) / results["Duration 2014-Holiday-Slow"]))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:200px; width:100%">
# MAGIC 
# MAGIC   <h3>What was the key performance difference between the two queries?</h3>
# MAGIC 
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Challenge #4
# MAGIC 
# MAGIC Why is **2017 Count Duration** [miniscully] faster than **2014 Count Duration** when there are more records in 2017?
# MAGIC 
# MAGIC Why is **2014 Holiday Duration** some 3x faster than **2017 Holiday Duration**
# MAGIC 
# MAGIC **Hint:** Explore the different facets of our app to see what accounts for the difference?
# MAGIC * Explore the data - is there a difference in the schema, number of records, datatypes, etc?
# MAGIC * Explore the code - is there something different between 2014 and 2017's code?
# MAGIC * Explore the data-on-disk - is there something about the files on disk?
# MAGIC * Did you rule out flukes in the benchmark by rerunning the queries 2-3 times to establish a baseline?
# MAGIC * Did you rule out a misconfiguration of the cluster (too much or too little RAM)?
# MAGIC * Always check the easy things first - save the time consuming investigation for later.
# MAGIC 
# MAGIC <p style="color:red">PLEASE don't give away the answer to your fellow students.<br/>
# MAGIC The purpose of this exercise it to learn how to diagnose the problem.</p>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>