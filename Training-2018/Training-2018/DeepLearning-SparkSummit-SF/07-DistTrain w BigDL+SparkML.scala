// Databricks notebook source
// MAGIC %md # Spark + Deep Learning?
// MAGIC 
// MAGIC __Training Full Models in Spark__
// MAGIC   * Intel BigDL - CPU focus
// MAGIC   * DeepLearning4J - GPU support
// MAGIC   * dist-keras - Keras API + distributed research-grade algorithms + GPU
// MAGIC   * TensorFlowOnSpark
// MAGIC   * Databricks - Spark Deep Learning Pipelines *(training coming very soon)*
// MAGIC   
// MAGIC __Transfer Learning__ (E.g., Neural Net as Featurizer + Spark Classifier)
// MAGIC   * Databricks - Spark Deep Learning Pipelines
// MAGIC   * Microsoft - MMLSpark
// MAGIC   
// MAGIC __Bulk Inference in Spark__
// MAGIC   * All of the above

// COMMAND ----------

// MAGIC %md ## Deep learning with BigDL on Databricks
// MAGIC 
// MAGIC [BigDL](https://github.com/intel-analytics/BigDL) is a distributed deep learning library built on Apache Spark. 
// MAGIC 
// MAGIC Built with Intel's MKL optimizations for scale-out on Xeon Phi (up to 72 cores / 3+ TFLOP) as an alternative to GPU. It will also run on regular MKL-capable x64 processors.
// MAGIC 
// MAGIC <img src="https://i.imgur.com/ALDqUfO.jpg" width=600>
// MAGIC 
// MAGIC In the latest releases, BigDL supports integration into Spark ML Pipelines, so we can use our familiar `DataFrame`-`Transformer`-`Estimator`-`Pipeline` patterns to train or predict using a deep learning model!
// MAGIC 
// MAGIC See also
// MAGIC * full Databricks [BigDL demo notebook](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5669198905533692/2626293254583012/3983381308530741/latest.html)
// MAGIC * Tutorials https://github.com/intel-analytics/BigDL-Tutorials
// MAGIC * and docs https://bigdl-project.github.io/master/
// MAGIC 
// MAGIC BigDL currently uses __synchronous SGD__ for optimization. Although there is a lot of interest in async, don't count sync out just yet! It won for Google in the 2016 paper "Revisiting Distributed Synchronous SGD" -- https://arxiv.org/pdf/1604.00981v2.pdf

// COMMAND ----------

// MAGIC %md ### Setting up BigDL on Databricks
// MAGIC 
// MAGIC __Create a library from Maven Coordinate `org.databricks.training:bigdl-SPARK_2.2:0.5.0`__
// MAGIC 
// MAGIC __and under Advanced Options, enter the repository URL: `https://s3-us-west-2.amazonaws.com/files.training.databricks.com/repo`__

// COMMAND ----------

// MAGIC %md ### Initialization
// MAGIC 
// MAGIC We start by initializing the BigDL Engine and setting up some parameters. The BigDL Engine expects two parameters:
// MAGIC 1. `nodeNumber`: the number of executor nodes
// MAGIC 2. `coreNumber`: the number of cores per executor node (expected to be uniform across executors)
// MAGIC 
// MAGIC BigDL will launch one task across all executors, where each executor runs a multi-threaded operation processing a part of the data.
// MAGIC 
// MAGIC This pattern for initializing will generate a warning, but we're using it because it allows dynamic config after startup (i.e., using the existing SparkConf/SparkConfig)

// COMMAND ----------

import com.intel.analytics.bigdl.utils.Engine
val nodeNumber = 1
val coreNumber = 8

Engine.init(nodeNumber, coreNumber, true /* env == "spark" */)

// COMMAND ----------

val train_libsvm = "/databricks-datasets/mnist-digits/data-001/mnist-digits-train.txt"
val test_libsvm = "/databricks-datasets/mnist-digits/data-001/mnist-digits-test.txt"

val dataset_train = spark.read.format("libsvm")
                              .option("numFeatures", "784")
                              .load(train_libsvm)
                              .selectExpr("features as raw", "(label + 1) as label") // Class labels must be 1-based

val dataset_test = spark.read.format("libsvm")
                              .option("numFeatures", "784")
                              .load(test_libsvm)
                              .selectExpr("features as raw", "(label + 1) as label")

// COMMAND ----------

display(dataset_train)

// COMMAND ----------

import org.apache.spark.ml.feature._

val scaler = new StandardScaler()
              .setWithMean(true).setWithStd(true)
              .setInputCol("raw").setOutputCol("features")

val scalerModel = scaler.fit(dataset_train)

val train = scalerModel.transform(dataset_train)

// COMMAND ----------

import com.intel.analytics.bigdl.models.lenet._

val lenet = LeNet5(10)  // 10 digit classes

// COMMAND ----------

import com.intel.analytics.bigdl.nn._
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat
import com.intel.analytics.bigdl.dlframes._

val estimator = new DLClassifier(lenet, ClassNLLCriterion[Float](), Array(784))
                  .setBatchSize(256)
                  .setMaxEpoch(4)

val dlModel = estimator.fit(train)

// COMMAND ----------

val predictions = dlModel.transform(scalerModel.transform(dataset_test))

// COMMAND ----------

import org.apache.spark.ml.evaluation._

val eval = new MulticlassClassificationEvaluator().setMetricName("accuracy")
eval.evaluate(predictions)

// COMMAND ----------

