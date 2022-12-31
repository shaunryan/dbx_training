// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ## SparkML
// MAGIC In this notebook, we'll use Spark for:
// MAGIC 
// MAGIC * Sentiment Analysis
// MAGIC * Natural Language Processing (NLP)
// MAGIC * Decision Trees
// MAGIC 
// MAGIC We will be using a dataset of roughly 50,000 IMDB reviews, which includes the English language text of that review and the rating associated with it (1-10). Based on the text of the review, we want to predict if the rating is "positive" or "negative".

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

val reviewsDF = spark.read.parquet("/mnt/training/movie-reviews/imdb/imdb_ratings_50k.parquet")
reviewsDF.createOrReplaceTempView("reviews")
display(reviewsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC What does the distribution of scores look like?
// MAGIC 
// MAGIC HINT: Use `count()`

// COMMAND ----------

// MAGIC %sql
// MAGIC --TODO: Replace <FILL IN> with appropriate code
// MAGIC 
// MAGIC SELECT <FILL_IN>

// COMMAND ----------

// MAGIC %md
// MAGIC The authors of this dataset have removed the "neutral" ratings, which they defined as a rating of 5 or 6.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Train-Test Split
// MAGIC 
// MAGIC We'll split our data into training and test samples. We will use 80% for training, and the remaining 20% for testing. We set a seed to reproduce the same results (i.e. if you re-run this notebook, you'll get the same results both times).

// COMMAND ----------

val Array(trainDF, testDF) = reviewsDF.randomSplit(Array(0.8, 0.2), seed=42)
trainDF.cache
testDF.cache

// COMMAND ----------

// MAGIC %md
// MAGIC Let's determine our baseline accuracy.

// COMMAND ----------

val positiveRatings = trainDF.filter("rating >= 5").count()
val totalRatings = trainDF.count()
val baselineAccuracy = positiveRatings.toDouble/totalRatings*100

println(f"Baseline accuracy: $baselineAccuracy%.2f%%")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Transformers
// MAGIC 
// MAGIC A transformer takes in a DataFrame, and returns a new DataFrame with one or more columns appended to it. They implement a `.transform()` method.

// COMMAND ----------

// MAGIC %md
// MAGIC Let's get started by using [RegexTokenizer](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.RegexTokenizer) to convert our review string into an array of tokens.

// COMMAND ----------

import org.apache.spark.ml._
import org.apache.spark.ml.feature._

val tokenizer = new RegexTokenizer()
  .setInputCol("review")
  .setOutputCol("tokens")
  .setPattern("\\W+")

val tokenizedDF = tokenizer.transform(trainDF)
display(tokenizedDF.limit(5)) // Look at a few tokenized reviews

// COMMAND ----------

// MAGIC %md
// MAGIC There are a lot of words that do not contain much information about the sentiment of the review (e.g. `the`, `a`, etc.). Let's remove these uninformative words using [StopWordsRemover](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.StopWordsRemover).

// COMMAND ----------

val remover = new StopWordsRemover()
  .setInputCol("tokens")
  .setOutputCol("stopWordFree")

val removedStopWordsDF = remover.transform(tokenizedDF)
display(removedStopWordsDF.limit(5)) // Look at a few tokenized reviews without stop words

// COMMAND ----------

// MAGIC %md
// MAGIC Where do the stop words actually come from? Spark includes a small English list as a default, which we're implicitly using here.

// COMMAND ----------

val stopWords = remover.getStopWords

// COMMAND ----------

// MAGIC %md
// MAGIC Let's remove the `br` from our reviews.

// COMMAND ----------

remover.setStopWords(Array("br") ++ stopWords)
val removedStopWordsDF = remover.transform(tokenizedDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Estimators
// MAGIC 
// MAGIC Estimators take in a DataFrame, and return a model (a Transformer). They implement a `.fit()` method.

// COMMAND ----------

// MAGIC %md
// MAGIC Let's apply a [CountVectorizer](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.CountVectorizer) model to convert our tokens into a vocabulary.

// COMMAND ----------

val counts = new CountVectorizer()
  .setInputCol("stopWordFree")
  .setOutputCol("features")
  .setVocabSize(1000)

val countModel = counts.fit(removedStopWordsDF) // It's a model

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC __Now let's adjust the label (target) values__
// MAGIC 
// MAGIC We want to group the reviews into "positive" or "negative" sentiment. So all of the star "levels" need to be collapsed into one of two groups. To accomplish this, we will use [Binarizer](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.Binarizer).

// COMMAND ----------

val binarizer = new Binarizer()
                    .setInputCol("rating")
                    .setOutputCol("label")
                    .setThreshold(5.0)

// COMMAND ----------

// MAGIC %md
// MAGIC Now we are going to use a Decision Tree model to fit to our dataset.

// COMMAND ----------

import org.apache.spark.ml.classification._

val dtc = new DecisionTreeClassifier()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Pipeline
// MAGIC 
// MAGIC Let's put all of these stages into a [Pipeline](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.Pipeline). This way, you don't have to remember all of the different steps you applied to the training set, and then apply the same steps to the test dataset. The pipeline takes care of that for you!

// COMMAND ----------

import org.apache.spark.ml.Pipeline

val pipeline = new Pipeline().setStages(Array(tokenizer, remover, counts, binarizer, dtc))
val pipelineModel = pipeline.fit(trainDF)

// COMMAND ----------

// MAGIC %md
// MAGIC We can extract the stages from our Pipeline, such as the Decision Tree model.

// COMMAND ----------

val decisionTree = pipelineModel.stages.last.asInstanceOf[DecisionTreeClassificationModel]
display(decisionTree)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's save the pipeline model.

// COMMAND ----------

val fileName = userhome + "/tmp/DT_Pipeline"
pipelineModel.write.overwrite().save(fileName)

// COMMAND ----------

// MAGIC %md
// MAGIC Now let's load the `PipelineModel` back in.
// MAGIC 
// MAGIC **Note**: You need to know what type of model you're loading in.

// COMMAND ----------

import org.apache.spark.ml.PipelineModel
// Load saved model
val savedPipelineModel = PipelineModel.load(fileName)

// COMMAND ----------

val resultDF = savedPipelineModel.transform(testDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Evaluate
// MAGIC 
// MAGIC We are going to use [MultiClassClassificationEvaluator](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator) to evaluate our predictions (we are using MultiClass because the BinaryClassificationEvaluator does not support accuracy as a metric).

// COMMAND ----------

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
println(s"Accuracy: ${evaluator.evaluate(resultDF)}")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Confusion Matrix
// MAGIC 
// MAGIC Let's see if we had more False Positive or False Negatives.

// COMMAND ----------

display(resultDF.groupBy("label", "prediction").count())

// COMMAND ----------

// MAGIC %md
// MAGIC In the next notebook, we will see how to apply this pipeline to streaming data!

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>