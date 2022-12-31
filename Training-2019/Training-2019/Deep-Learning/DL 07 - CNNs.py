# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #Convolutional Neural Networks
# MAGIC 
# MAGIC We will use pre-trained Convolutional Neural Networks (CNNs), trained with the image dataset from [ImageNet](http://www.image-net.org/), to demonstrate two aspects. First, how to explore and classify images. And second, how to use transfer learning with existing trained models (next lab).
# MAGIC 
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Analyze popular CNN architectures
# MAGIC  - Apply pre-trained CNNs to images using Pandas Scalar Iterator UDF

# COMMAND ----------

# MAGIC %md
# MAGIC ## VGG16
# MAGIC ![vgg16](https://neurohive.io/wp-content/uploads/2018/11/vgg16-neural-network.jpg)
# MAGIC 
# MAGIC We are going to start with the VGG16 model, which was introduced by Simonyan and Zisserman in their 2014 paper [Very Deep Convolutional Networks for Large Scale Image Recognition](https://arxiv.org/abs/1409.1556).
# MAGIC 
# MAGIC Let's start by downloading VGG's weights and model architecture.

# COMMAND ----------

from tensorflow.keras.preprocessing import image
from tensorflow.keras.applications.vgg16 import preprocess_input, decode_predictions, VGG16
import numpy as np
import os

vgg16Model = VGG16(weights='imagenet')

# COMMAND ----------

# MAGIC %md
# MAGIC We can look at the model summary. Look at how many parameters there are! Imagine if you had to train all 138,357,544 parameters from scratch! This is one motivation for re-using existing model weights.
# MAGIC 
# MAGIC **RECAP**: What is a convolution? Max pooling?

# COMMAND ----------

vgg16Model.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC **Question**: What do the input and output shapes represent?

# COMMAND ----------

# MAGIC %md
# MAGIC In Tensorflow, it represents the images in a channels-last manner: (samples, height, width, color_depth)
# MAGIC 
# MAGIC But in other frameworks, such as Theano, the same data would be represented channels-first: (samples, color_depth, height, width)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply pre-trained model
# MAGIC 
# MAGIC We are going to make a helper method to resize our images to be 224 x 224, and output the top 3 classes for a given image.

# COMMAND ----------

def predict_images(images, model):
  for i in images:
    print(f"Processing image: {i}")
    img = image.load_img(i, target_size=(224, 224))
    #convert to numpy array for Keras image formate processing
    x = image.img_to_array(img)
    x = np.expand_dims(x, axis=0)
    x = preprocess_input(x)
    preds = model.predict(x)
    # decode the results into a list of tuples (class, description, probability)
    print('Predicted:', decode_predictions(preds, top=3)[0], '\n')

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Images
# MAGIC <div style="text-align: left; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://files.training.databricks.com/images/pug.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC   <img src="https://files.training.databricks.com/images/strawberries.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC   <img src="https://files.training.databricks.com/images/rose.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC   
# MAGIC </div>
# MAGIC 
# MAGIC Let's make sure the datasets are already mounted.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

img_paths = ["/dbfs/mnt/training/dl/img/pug.jpg", "/dbfs/mnt/training/dl/img/strawberries.jpg", "/dbfs/mnt/training/dl/img/rose.jpg"]
predict_images(img_paths, vgg16Model)

# COMMAND ----------

# MAGIC %md
# MAGIC The network did so well with the pug and strawberry! What happened with the rose? Well, it turns out that `rose` was not one of the 1000 categories that VGG16 had to predict. But it is quite interesting it predicted `sea_anemone` and `vase`.

# COMMAND ----------

# MAGIC %md
# MAGIC You can play around with this with your own images by doing the following:
# MAGIC 
# MAGIC Get a new file: 
# MAGIC 
# MAGIC `%sh wget image_url.jpg`
# MAGIC 
# MAGIC `%fs cp file:/databricks/driver/image_name.jpg yourName/tmp/image_name.jpg `
# MAGIC 
# MAGIC 
# MAGIC OR
# MAGIC 
# MAGIC You can upload this file via the Data UI and read in from the FileStore path (e.g. `/dbfs/FileStore/image_name.jpg`).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Classify Co-Founders of Databricks
# MAGIC <div style="text-align: left; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://files.training.databricks.com/images/Ali-Ghodsi-4.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC   <img src="https://files.training.databricks.com/images/andy-konwinski-1.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC   <img src="https://files.training.databricks.com/images/ionS.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC   <img src="https://files.training.databricks.com/images/MateiZ.jpg" height="200" width="150" alt="Databricks Nerds!" style=>
# MAGIC   <img src="https://files.training.databricks.com/images/patrickW.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC   <img src="https://files.training.databricks.com/images/Reynold-Xin.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Load these images into a DataFrame.

# COMMAND ----------

df = spark.read.format("image").load("mnt/training/dl/img/founders/")
df.cache().count()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's wrap the prediction code inside a UDF so we can apply this model in parallel on each row of the DataFrame.

# COMMAND ----------

from pyspark.sql.types import StringType, ArrayType

# Helper func to return top3 results as strings in an array
def get_results(path, model, preprocess_input, decode_predictions, target_size=(224,224)):
  img = image.load_img(path, target_size=target_size)

  x = image.img_to_array(img)
  x = np.expand_dims(x, axis=0)
  x = preprocess_input(x)
  preds = model.predict(x)

  # decode the results into a list of tuples (class, description, probability)  
  top_3 = decode_predictions(preds, top=3)[0]
  result = []
  for _, label, prob in top_3:
    result.append(f"{label}: {prob:.3f}")
  return result

# Define udf to do preprocessing and prediction steps
@udf(ArrayType(StringType()))
def vgg16_predict_udf(image_data):
  path = image_data[0].replace("dbfs:", "/dbfs")
  model = VGG16(weights='imagenet')
  result = get_results(path, model, preprocess_input, decode_predictions)
  return result

display(df.withColumn("Top 3 VGG16 Predictions", vgg16_predict_udf("image")))

# COMMAND ----------

# MAGIC %md
# MAGIC **[Pandas Scalar Iterator UDF](https://docs.databricks.com/spark/latest/spark-sql/udf-python-pandas.html#scalar-iterator-udfs)**
# MAGIC 
# MAGIC While the above UDF code is simpler to write and understand, when using a dataset with more images, it is more performant to use a different type of UDF, the pandas scalar iterator UDF. The following cell demonstrates how to use this type of UDF on our data.
# MAGIC 
# MAGIC A pandas scalar iterator UDF takes in an iterator of pandas series so instead of making predictions one image at a time, we can apply functions to an entire pandas series at once. This way the cost of any set-up needed (like loading the VGG16 model in our case) will be incurred fewer times. When the number of images you’re working with is greater than `spark.conf.get('spark.sql.execution.arrow.maxRecordsPerBatch')`, which is 10,000 by default, you'll see significant speed ups over a pandas scalar UDF because it iterates through batches of pd.Series.
# MAGIC 
# MAGIC Note: The runtime of the two UDF cells are dependent on each other. If the workers cached the model weights after loading it in the first time, subsequent calls of the same UDF/UDF with the same load model code will become significantly faster.

# COMMAND ----------

import pandas as pd
import numpy as np

## Pandas UDF Scalar Iter ##
from pyspark.sql.functions import col, pandas_udf, struct, PandasUDFType
from pyspark.sql.types import StringType, ArrayType

def preprocess(image_path):
  path = image_path.replace("dbfs:", "/dbfs")
  img = image.load_img(path, target_size=(224, 224))
  
  x = image.img_to_array(img)
  x = preprocess_input(x)
  return x

@pandas_udf(ArrayType(StringType()), PandasUDFType.SCALAR_ITER)
def vgg16_predict_pandas_udf(image_data_iter):
  model = VGG16(weights='imagenet') # load model outside of for loop
  for image_data_series in image_data_iter:
    image_path_series = image_data_series["origin"]
    
    # apply functions to entire series at once
    x = image_path_series.map(preprocess) 
    x = np.stack(list(x.values))
    preds = model.predict(x, batch_size=6)
    top_3s = decode_predictions(preds, top=3)
    
    # format results
    results = []
    for top_3 in top_3s:
      result = []
      for _, label, prob in top_3:
        result.append(f"{label}: {prob:.3f}")
      results.append(result)
    yield pd.Series(results)

display(df.withColumn("Top 3 VGG16 Predictions (pandas udf)", vgg16_predict_pandas_udf("image")))

# COMMAND ----------

# MAGIC %md
# MAGIC Yikes! These are not the most natural predictions (because ImageNet did not have a `person` category). In the next lab, we will cover how to utilize existing components of the VGG16 architecture, and how to retrain the final classifier.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>