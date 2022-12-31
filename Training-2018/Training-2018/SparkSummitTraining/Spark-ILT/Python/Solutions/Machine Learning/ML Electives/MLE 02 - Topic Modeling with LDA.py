# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Topic Modeling with Latent Dirichlet Allocation (LDA)
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/topic-modeling-with-lda.png">
# MAGIC 
# MAGIC In this lab, our task is to identify topics from a collection of text documents. To accomplish this, we will use the Latent Dirichlet Allocation (LDA) algorithm. It accepts a vectors of word counts and outputs the respective topics for the documents.
# MAGIC 
# MAGIC Spark API docs
# MAGIC - Scala: [LDA](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.clustering.LDA)
# MAGIC - Python: [LDA](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.LDA)
# MAGIC - [ML Programming Guide](http://spark.apache.org/docs/latest/ml-guide.html)
# MAGIC 
# MAGIC Wikipedia: [Latent Dirichlet Allocation](https://en.wikipedia.org/wiki/Latent_Dirichlet_allocation)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Reading the data

# COMMAND ----------

# MAGIC %md
# MAGIC The data we are working with contains messages from Usenet ([NNTP](https://en.wikipedia.org/wiki/Network_News_Transfer_Protocol)) newsgroups. Each message is part of a topic.
# MAGIC 
# MAGIC Watch out! LDA is an unsupervised machine learning algorithm, so we won't use the `topic` column when we train the LDA model. We would rather drop it in the next few cells.

# COMMAND ----------

corpusRawDF = spark.read.parquet("/databricks-datasets/news20.binary/data-001/training")

display(corpusRawDF)

# COMMAND ----------

# MAGIC %md
# MAGIC How many articles and topics are in our DataFrame?

# COMMAND ----------

messageCnt = corpusRawDF.count()
topicCount = corpusRawDF.select("topic").distinct().count()
print("The DataFrame contains {0} emails accross {1} topics.".format( messageCnt, topicCount ))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's see the actual topics

# COMMAND ----------

topicsDF = corpusRawDF.select("topic").distinct()
display(topicsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Data Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tokenizing the messages
# MAGIC 
# MAGIC Only keep words at least 4 characters long and create tokens in the `tokens` column.

# COMMAND ----------

from pyspark.ml.feature import RegexTokenizer

tokenizer = (RegexTokenizer()
            .setPattern("[\\W_]+")
            .setMinTokenLength(4) # Filter away tokens with length < 4
            .setInputCol("text")
            .setOutputCol("tokens"))

tokenizedDF = tokenizer.transform(corpusRawDF)

display(tokenizedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Removing Stopwords
# MAGIC 
# MAGIC LDA will look for dominant words in messages. In order to decrease the noise, we will remove [the English stopwords](https://spark.apache.org/docs/latest/ml-features.html#stopwordsremover) and also words appearing in the majority of messages.

# COMMAND ----------

from pyspark.ml.feature import StopWordsRemover

remover = (StopWordsRemover()
          .setInputCol("tokens")
          .setOutputCol("noStopWords"))

stopwords = remover.getStopWords()
print(stopwords)

# COMMAND ----------

# Stopwords specific to NNTP
nntp_stopwords = ["writes", "subject", "from", "organization", "line", "lines", "entry", "date", "nntp", "host", "reply", "posting", "newsgroup"]

# Additional common words in this corpus
additional_stopwords = ["article", "entry", "udel", "said", "tell", "think", "know", "just", "like", "does", "going", "make", "thanks", "people"]

# Update the stopwords dictionary and apply the StopWordsRemover
new_stopwords = stopwords + nntp_stopwords + additional_stopwords
remover.setStopWords(new_stopwords)

noStopWordsDF = remover.transform(tokenizedDF)

# Show a sample of short messages on the screen and see which words got removed.
shortMessagesDF = noStopWordsDF.filter(length(col("text")) < 200)
display(shortMessagesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Vector of Token Counts
# MAGIC 
# MAGIC LDA takes in a vector of token counts as input. We can use the CountVectorizer() to easily convert our text documents into vectors of token counts. 
# MAGIC [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.CountVectorizer) and [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.CountVectorizer) docs for CountVectorizer.
# MAGIC 
# MAGIC The CountVectorizer will return (VocabSize, Array(Indexed Tokens), Array(Token Frequency)). 
# MAGIC 
# MAGIC Two handy parameters to note:
# MAGIC 
# MAGIC `setMinDF: Specifies the minimum number of different documents a term must appear in to be included in the vocabulary.`
# MAGIC `setMinTF: Specifies the minimum number of times a term has to appear in a document to be included in the vocabulary.`
# MAGIC 
# MAGIC Set the parameters of CountVectorizer so that we have a vocab size of `1000`, the input column will be called `noStopWords`, the output column will be `features` & the minimum document frequency is `5`.

# COMMAND ----------

# ANSWER

from pyspark.ml.feature import CountVectorizer

vectorizer = (CountVectorizer()
            .setInputCol("noStopWords")
            .setOutputCol("features")
            .setVocabSize(1000)
            .setMinDF(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Model Training
# MAGIC 
# MAGIC Let's train our Pipeline. First, we are creating an LDA model with *20* topics. Remember, our corpus comes from 20 different Usenet topics, as we saw it at the beginning of this notebook. 
# MAGIC 
# MAGIC Here we are setting the `maxIter` parameter to *5* so that the training finishes fast. In a production setting you might want to increase this number to arrive at more accurate clusters. 
# MAGIC 
# MAGIC Once we are done, we put the transformers, the estimators and LDA in a pipeline and train the pipeline.

# COMMAND ----------

from pyspark.ml.clustering import LDA

# Set LDA params
numTopics = 20
seed = 273

lda = (LDA()
      .setMaxIter(5)
      .setK(numTopics)
      .setSeed(seed))

# COMMAND ----------

# MAGIC %md
# MAGIC Create a machine learning pipeline with the *Tokenizer*, *StopWordsRemover*, *CountVectorizer* and *LDA*. Fit it to `corpusRawDF`.
# MAGIC 
# MAGIC *Training the pipeline can take up to 1-2 minutes.* 

# COMMAND ----------

# ANSWER

from pyspark.ml import Pipeline

pipeline = Pipeline()
pipeline.setStages([tokenizer, remover, vectorizer, lda])
pipelineModel = pipeline.fit(corpusRawDF)

ldaModel = pipelineModel.stages[-1]

# COMMAND ----------

# MAGIC %md
# MAGIC The resulting model contains info about the typical terms for each topic and their weights respectively.

# COMMAND ----------

topicsAndTermsDF = ldaModel.describeTopics(maxTermsPerTopic=3)
display(topicsAndTermsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Visualizing the Results
# MAGIC 
# MAGIC The LDA model contains 20 rows, one row for each topic. We are pulling `topicsAndTemsDF` back to the driver into a native collection, convert  the term indices to words with the help of the `vectorizer`'s vocabulary. Finally generate a word cloud with the dominant words for each topic.

# COMMAND ----------

topicsLocal = topicsAndTermsDF.collect()
print(topicsLocal[0])

# COMMAND ----------

vectorizerModel = pipelineModel.stages[-2] # The model of the vectorizer, which contains the termIndex-term vocabulary
voc = vectorizerModel.vocabulary

topicTermsLocal = []
for row in topicsLocal:
  for term in zip(row['termIndices'], row['termWeights']):
    topicTermsLocal.append({
      'topicId':    row['topic'],
      'term': voc[term[0]],
      'probability':   term[1]})

print("\n".join(map(str, topicTermsLocal)))

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, convert the `topicTermsLocal` to JSON format to visualize it. 

# COMMAND ----------

import json

raw_json = json.dumps(topicTermsLocal)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Showing the word cloud with the D3 Javascript library
# MAGIC 
# MAGIC - Colors are representing topics
# MAGIC - Bubble sizes are representing how much a term contributes to a topic

# COMMAND ----------

displayHTML("""
<!DOCTYPE html>
<meta charset="utf-8">
<style>
circle {
  fill: rgb(31, 119, 180);
  fill-opacity: 0.5;
  stroke: rgb(31, 119, 180);
  stroke-width: 1px;
 }
.leaf circle {
  fill: #ff7f0e;
  fill-opacity: 1;
 }
text {
  font: 14px sans-serif;
 }
</style>
<body>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.5/d3.min.js"></script>
<script>

var json = {
 "name": "data",
 "children": [
  {
     "name": "topics",
     "children": 
      """ + raw_json + """
     
    }
   ]
};

var r = 1000,
    format = d3.format(",d"),
    fill = d3.scale.category20c();

var bubble = d3.layout.pack()
    .sort(null)
    .size([r, r])
    .padding(1.5);

var vis = d3.select("body").append("svg")
    .attr("width", r)
    .attr("height", r)
    .attr("class", "bubble");

var node = vis.selectAll("g.node")
    .data(bubble.nodes(classes(json))
    .filter(function(d) { return !d.children; }))
    .enter().append("g")
    .attr("class", "node")
    .attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; })
    color = d3.scale.category20();

  node.append("title")
      .text(function(d) { return d.className + ": " + format(d.value); });

  node.append("circle")
      .attr("r", function(d) { return d.r; })
      .style("fill", function(d) {return color(d.topicName);});

var text = node.append("text")
    .attr("text-anchor", "middle")
    .attr("dy", ".3em")
    .text(function(d) { return d.className.substring(0, d.r / 3)});

  text.append("tspan")
      .attr("dy", "1.2em")
      .attr("x", 0)
      .text(function(d) {return Math.ceil(d.value * 10000) /10000; });

// Returns a flattened hierarchy containing all leaf nodes under the root.

function classes(root) {
  var classes = [];

  function recurse(term, node) {
    if (node.children) node.children.forEach(function(child) { recurse(node.term, child); });
    else classes.push({topicName: node.topicId, className: node.term, value: node.probability});
  }

  recurse(null, root);
  return {children: classes};
 }
</script>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Where to go from here?

# COMMAND ----------

# MAGIC %md
# MAGIC To improve our results further, we could employ some of the below methods:
# MAGIC 
# MAGIC - Refilter data for additional data-specific stopwords
# MAGIC - Use Stemming or Lemmatization to preprocess data
# MAGIC - Experiment with a smaller number of topics, since some of these topics in the 20 Newsgroups are pretty similar
# MAGIC - Increase model's MaxIterations

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>