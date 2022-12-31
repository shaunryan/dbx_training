# Databricks notebook source
# MAGIC %md ##Vectorized PandasUDF + Keras for Inference
# MAGIC 
# MAGIC One of the exciting things we can do with Arrow/Vectorized PandasUDF is efficiently integrate Spark with numeric or even GPU code that was *not* designed with Spark in mind.
# MAGIC 
# MAGIC For example, we might have a model that we've build with Keras+TensorFlow -- without Spark in the picture at all -- and then efficiently use that model to perform inference on big datasets with Spark.
# MAGIC 
# MAGIC In this module, we'll do just that: we'll train a simple neural network using Keras, save it to disk, and then use it from Spark via PandasUDF.

# COMMAND ----------

# MAGIC %md For this notebook, we'll need the following libraries, which we can install from PyPI via Databricks "Create Library":
# MAGIC * `tensorflow`
# MAGIC * `keras`
# MAGIC * `h5py`

# COMMAND ----------

# MAGIC %md We'll start by using Pandas to read our Diamonds dataset, and to save time (and featurization) we'll just use the 6 continuous variables in our model to predict price.

# COMMAND ----------

import pandas as pd
import IPython.display as disp

input_file = "/dbfs/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"

df = pd.read_csv(input_file, header = 0)
df.drop(df.columns[0], axis=1, inplace=True)
df.drop(df.columns[1:4], axis=1, inplace=True)
disp.display(df)

# COMMAND ----------

# MAGIC %md We'll do a train/test split, and look at a few rows as a sanity check:

# COMMAND ----------

from sklearn.model_selection import train_test_split

X = df.drop(df.columns[3], axis=1).as_matrix()
y = df.iloc[:,3:4].as_matrix().flatten()

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

print(X[:5])

print(y[:5])

# COMMAND ----------

# MAGIC %md Now we'll build a simple feed-forward perceptron network in Keras, and train it for a minute or so, then check our performance

# COMMAND ----------

from keras.models import Sequential
from keras.layers import Dense
import numpy as np

model = Sequential()
model.add(Dense(16, input_dim=6, kernel_initializer='normal', activation='relu')) 
model.add(Dense(1, kernel_initializer='normal', activation='linear'))

model.compile(loss='mean_squared_error', optimizer='adam', metrics=['mean_squared_error'])
history = model.fit(X_train, y_train, epochs=100, batch_size=128, validation_split=0.1, verbose=2)

scores = model.evaluate(X_test, y_test)
print("\nroot %s: %f" % (model.metrics_names[1], np.sqrt(scores[1])))

# COMMAND ----------

model.save("/tmp/model1")

# COMMAND ----------

# MAGIC %sh cp /tmp/model1 /dbfs/tmp/model1

# COMMAND ----------

# MAGIC %md Ok, now let's get Spark DataFrame with our test data. In real life, we'd read this directly from S3, HDFS, Kafka, or wherever.
# MAGIC 
# MAGIC But since this test set is already on the driver (and not very big), we can make a distributed Spark DF from the Pandas DF.
# MAGIC 
# MAGIC __In order to "keep it real" we're doing to use a Spark VectorAssembler to put the features into a single column of Vector.__
# MAGIC 
# MAGIC This is key because in real (Spark) life, you'll almost always have a single column with a Vector in it, and making that integration work is key to making this pattern useful. (*If we only wanted to work with 6 predictors, like we have here, we could just pass the columns directly, but that's sort of tennis-without-the-net*)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

testDF = spark.createDataFrame(pd.DataFrame(X_test, columns=["carat", "depth", "table", "x", "y", "z"]))

testDF = VectorAssembler(inputCols=testDF.columns, outputCol="features").transform(testDF)

display(testDF)

# COMMAND ----------

# MAGIC %md Now we have a realistic situation in Spark, from which we'd like to do inference. 
# MAGIC 
# MAGIC In Spark 2.3, we need to convert those feature vectors to Array objects in order to get them to marshal through Arrow. This should change in the future, but for now we can write a Scala (for performance) user-defined function (UDF) to help.

# COMMAND ----------

# MAGIC %scala spark.udf.register("to_array", (x:org.apache.spark.ml.linalg.Vector) => x.toArray)

# COMMAND ----------

# MAGIC %md It's hard to debug in the guts of Spark or on the executors, so let's sort out some data conversion code here.
# MAGIC 
# MAGIC When our vector column ("features") comes through into our PandasUDF, it will show up as a pd.Series of lists. We need to reshape this a little to make a regular numpy array.
# MAGIC 
# MAGIC To illustrate the pattern, here's local equivalent:

# COMMAND ----------

import pandas as pd
s = pd.Series([[1, 2, 3], [4, 5, 6]])
sm = np.stack(s.values)
print(sm)
print(type(sm))

# COMMAND ----------

# MAGIC %md Ok, so that will get us a regular numpy array, perfect for doing predictions with Keras. 
# MAGIC 
# MAGIC Let's see what the output from Keras' `model.predict` will look like:

# COMMAND ----------

model.predict(X_test[:3].reshape(3,6))

# COMMAND ----------

# MAGIC %md That's almost perfect ... but Spark is going to expect a flat array of outputs from the PandasUDF (one for each input)

# COMMAND ----------

model.predict(X_test[:3].reshape(3,6)).flatten()

# COMMAND ----------

# MAGIC %md Hopefully with that out of the way, the following implementation (which uses those reshape tricks above) will work.
# MAGIC 
# MAGIC *Note: in real life, don't load the model on each call ... make sure the model is available ahead of time on the executors and just loaded once (e.g. by distributing a module/pyfile) ... data engineering can help with this*

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import PandasUDFType
import keras.models

@pandas_udf("double", PandasUDFType.SCALAR)
def keras_predict(v):    
    keras_model = keras.models.load_model('/dbfs/tmp/model1')
    s = np.stack(v.values)
    return pd.Series(keras_model.predict(s).flatten())

# COMMAND ----------

# MAGIC %md Now we're ready to try it! We're going to use `selectExpr` to call our `to_array` helper via its SQL name:

# COMMAND ----------

with_features_as_array = testDF.selectExpr("to_array(features) AS features_as_array")

display(with_features_as_array)

# COMMAND ----------

# MAGIC %md And now we can `keras_predict` on the column:

# COMMAND ----------

with_features_as_array.select(keras_predict("features_as_array")).show()

# COMMAND ----------

# MAGIC %md It's not as complicated as it might seem -- I just wanted to show all of the steps to make it extra clear. And now ... we've got Spark and all of our Python goodness playing nice!

# COMMAND ----------

# MAGIC %md ####Awesome ... But What about Distributed Deep Learning Training??
# MAGIC 
# MAGIC __Training Full Models in Spark__
# MAGIC   * Intel BigDL - CPU focus
# MAGIC   * DeepLearning4J - GPU support
# MAGIC   * dist-keras - Keras API + distributed research-grade algorithms + GPU
# MAGIC   * TensorFlowOnSpark
# MAGIC   * Databricks - Spark Deep Learning Pipelines
# MAGIC   
# MAGIC __Transfer Learning__ (E.g., Neural Net as Featurizer + Spark Classifier)
# MAGIC   * Databricks - Spark Deep Learning Pipelines
# MAGIC   * Microsoft - MMLSpark
# MAGIC 
# MAGIC (incidentally, all of those can also do bulk/batch inference easily as well)
# MAGIC 
# MAGIC ####We'll look at some distributed DL with BigDL as well as with dist-keras in the next modules

# COMMAND ----------

