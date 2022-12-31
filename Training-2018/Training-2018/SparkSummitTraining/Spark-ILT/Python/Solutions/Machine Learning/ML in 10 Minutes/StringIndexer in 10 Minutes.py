# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # StringIndexer
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Introduction
# MAGIC 
# MAGIC Many times our input data contains string-typed columns. Machine Learning algorithms favour double typed columns though. To bridge this gap, Spark provides the `StringIndexer` class, which takes string-types columns and encodes these strings into double-typed columns. Spark also provides the `IndexToString` class to "decode" indices.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/string-indexer.png">

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Documentation

# COMMAND ----------

# MAGIC %md
# MAGIC * <a href='https://spark.apache.org/docs/latest/ml-features.html#stringindexer' target="_blank">StringIndexer documentation</a> in the Spark ML programming guide
# MAGIC * <a href='https://spark.apache.org/docs/latest/ml-features.html#indextostring' target="_blank">IndexToString documentation</a> in the Spark ML programming guide

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) StringIndexer in Action

# COMMAND ----------

# MAGIC %md
# MAGIC Let's create a DataFrame with a column containing categorical values:

# COMMAND ----------

from pyspark.sql import Row

d = [  
  Row(1,"red"),
  Row(2,"red"),
  Row(3,"red"),
  Row(4,"white"),
  Row(5,"green"),
  Row(6,"green"),
  Row(7,"red"),
  Row(8,"white"),
  Row(9,"green")
]

df = spark.createDataFrame(d,["id", "category"])
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Now, import the *StringIndexer* class and take a look at the its parameters:

# COMMAND ----------

from pyspark.ml.feature import StringIndexer
indexer = StringIndexer()

print(indexer.explainParams())

# COMMAND ----------

# MAGIC %md
# MAGIC Go and get the *StringIndexer* create a *category*-*index* mapping:

# COMMAND ----------

(indexer
   .setInputCol("category")
   .setOutputCol("indexedCategory")
)
indexerModel = indexer.fit(df)

labels = indexerModel.labels
print(labels)

# COMMAND ----------

# MAGIC %md
# MAGIC The string indexes correspond to the array indexes of `indexerModel.labels`. By default the most frequent value comes first.

# COMMAND ----------

for i in range(0,len(labels)):
  print("{} : {}".format(i, labels[i]))

# COMMAND ----------

# MAGIC %md
# MAGIC Now, do the indexing:

# COMMAND ----------

indexedDF = indexerModel.transform(df)
display(indexedDF)

# COMMAND ----------

indexedDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) IndexToString
# MAGIC 
# MAGIC The *IndexToString* class takes an indexed column and decodes the indices to their original string values. 
# MAGIC 
# MAGIC It does it by pulling the data from the indexed column's metadata:

# COMMAND ----------

indexedDF.schema[2].metadata

# COMMAND ----------

from pyspark.ml.feature import IndexToString

converter = IndexToString(inputCol="indexedCategory", outputCol="originalCategory")
convertedDF = converter.transform(indexedDF)

display(convertedDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>