// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC # One-hot Encoding
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Introduction
// MAGIC 
// MAGIC One-hot encoding converts categorical variables into a form that enables some ML algorithms, like Linear Regresion, to make better predictions.
// MAGIC 
// MAGIC It creates new, binary columns, which indicate the presence of each possible value from the original data.
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/one-hot-encoding.png">

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Documentation

// COMMAND ----------

// MAGIC %md
// MAGIC * <a href='https://spark.apache.org/docs/latest/ml-features.html#onehotencoderestimator' target="_blank">OneHotEncoderEstimator documentation</a> in the Spark ML programming guide

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) One-hot encoding in Action

// COMMAND ----------

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

val d = List(
  Row(1,1),
  Row(2,2),
  Row(3,3),
  Row(4,1),
  Row(5,1),
  Row(6,2)
)

val schema = StructType(Seq(
  StructField("id", IntegerType, true),
  StructField("category", IntegerType, true)
))
val df = spark.createDataFrame(sc.parallelize(d), schema)
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC Now, import the *OneHotEncoderEstimator* class and take a look at the its parameters:

// COMMAND ----------

import org.apache.spark.ml.feature.OneHotEncoderEstimator
val encoder = new OneHotEncoderEstimator()

encoder.explainParams()

// COMMAND ----------

// MAGIC %md
// MAGIC Set the `inputCols` and the `outputCols` parameter and do the encoding. These parameters can accept multiple columns, if needed. Spark ML uses vectors for model building, therefore Spark stores the output in a vector typed column.

// COMMAND ----------

(encoder
  .setInputCols(Array("category"))
  .setOutputCols(Array("categoryVec")))

val encodedDF = encoder.fit(df).transform(df)

display(encodedDF)

// COMMAND ----------

// MAGIC %md
// MAGIC The above line is a <a href="https://spark.apache.org/docs/latest/mllib-data-types.html#local-vector">SparseVector</a> representation of the three columns we are recreating below.

// COMMAND ----------

import org.apache.spark.sql.functions.{udf, lit}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.linalg.SparseVector

val getAtPos = spark.udf.register("getAtPos", (v:SparseVector, i:Integer) => {
  if ((i == 3 && v(1) + v(2) == 0) || (i < 3 && v(i) == 1)) 1 else 0 
})

display(encodedDF.select(
  $"id",
  $"category",
  getAtPos($"categoryVec", lit(1)).as("categoryVec_1"),
  getAtPos($"categoryVec", lit(2)).as("categoryVec_2"),
  getAtPos($"categoryVec", lit(3)).as("categoryVec_3")
))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>