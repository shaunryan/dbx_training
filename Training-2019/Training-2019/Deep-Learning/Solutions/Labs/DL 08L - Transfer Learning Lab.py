# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # X-Ray Vision with Transfer Learning
# MAGIC 
# MAGIC In this notebook, we will use Transfer Learning on images of chest x-rays to predict if a patient has pneumonia (bacterial or viral) or is normal. 
# MAGIC 
# MAGIC This data set was obtained from [http://dx.doi.org/10.17632/rscbjbr9sj.2](http://dx.doi.org/10.17632/rscbjbr9sj.2). The source of the data is:
# MAGIC Kermany, Daniel; Zhang, Kang; Goldbaum, Michael (2018), “Labeled Optical Coherence Tomography (OCT) and Chest X-Ray Images for Classification”, Mendeley Data, v2
# MAGIC http://dx.doi.org/10.17632/rscbjbr9sj.2
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Build a model with nearly perfect accuracy predicting if a patient has pneumonia or not using transfer learning
# MAGIC  
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **NOTE**: The normal and pneumonia images are located in different directories, so we will have to union them together. We will adjust the test dataset to be roughly 50/50 as well because the test dataset does not contain the true distribution of folks that have pneumonia.
# MAGIC 
# MAGIC We are not using sampleBy here, because the readImages does not push the sample filter down to source.

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

from pyspark.ml.image import ImageSchema
from pyspark.sql.functions import lit
import tensorflow as tf
import numpy as np
np.random.seed(0)
tf.set_random_seed(0)

normal_train = spark.read.format("image").load("/mnt/training/chest-xray/train/normal/").withColumn("label", lit(0)).sample(False, .05, seed=42)
normal_test = spark.read.format("image").load("/mnt/training/chest-xray/test/normal/").withColumn("label", lit(0)).sample(False, .1, seed=42)

pneumonia_train = spark.read.format("image").load("/mnt/training/chest-xray/train/pneumonia/").withColumn("label", lit(1)).sample(False, .01, seed=42)
pneumonia_test = spark.read.format("image").load("/mnt/training/chest-xray/test/pneumonia/").withColumn("label", lit(1)).sample(False, .04, seed=42)

train_df = normal_train.union(pneumonia_train).cache()
test_df = normal_test.union(pneumonia_test).cache()

print(f"Number of records in the training set: {train_df.count()}")
print(f"Number of records in the test set: {test_df.count()}")
print(f"Fraction pneumonia in the training set: {pneumonia_train.count()/(normal_train.count()+pneumonia_train.count())}")
print(f"Fraction pneumonia in the test set: {pneumonia_test.count()/(normal_test.count()+pneumonia_test.count())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize the Images

# COMMAND ----------

display(train_df.sample(.10))

# COMMAND ----------

# MAGIC %md
# MAGIC We'll need to convert the image paths into a Pandas DataFrame to work with Keras.

# COMMAND ----------

train_data = train_df.toPandas()
train_data['path'] = train_data['image'].apply(lambda x: x["origin"].replace("dbfs:/","/dbfs/"))
train_data['label'] = train_data['label'].astype(str)

test_data = test_df.toPandas()
test_data['path'] = test_data['image'].apply(lambda x: x["origin"].replace("dbfs:/","/dbfs/"))
test_data['label'] = test_data['label'].astype(str)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build and Finetune the Model
# MAGIC 
# MAGIC We are going to use a pretrained VGG16 model. We will apply all layers of the neural network, but remove the last layer. We can then re-train the last layer with a sigmoid activation, which acts as a logisitic regression model on the CNN output.

# COMMAND ----------

# ANSWER
from keras import applications
from keras.preprocessing.image import ImageDataGenerator
from keras.models import Model 
from keras.layers import Dense

img_width = 224
img_height = 224

# Load original model with pretrained weights from imagenet
original_model = applications.VGG16(weights = "imagenet", input_shape = (img_height, img_width, 3))

# Remove last layer of predictions to make model
original_model.layers.pop() 
model = Model(original_model.input, original_model.layers[-1].output)

# Freeze all the original trained layers so we can reuse them
for layer in model.layers:
    layer.trainable = False
    
# Add custom new layer for binary cat/dog output
x = model.output
predictions = Dense(1, activation="sigmoid")(x)

model_final = Model(inputs = model.input, outputs = predictions)
model_final.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's compile and train the model. 

# COMMAND ----------

# ANSWER
# Compile the model
model_final.compile(loss="binary_crossentropy", optimizer="adam", metrics=["accuracy"])

# Loading training data
datagen = ImageDataGenerator(preprocessing_function = applications.vgg16.preprocess_input)
train_generator = datagen.flow_from_dataframe(dataframe=train_data, directory=None, x_col="path", 
                                              y_col="label", class_mode="binary", target_size = (img_height, img_width))

print(f"Class labels: {train_generator.class_indices}")

# Train the model 
model_final.fit_generator(train_generator, steps_per_epoch=1, epochs=19)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate the model
# MAGIC 
# MAGIC From above, we've established our baseline accuracy. If we always predict `normal` on the test set data, we will have 50% accuracy. Let's see how we did in comparison!

# COMMAND ----------

# ANSWER
test_datagen = ImageDataGenerator(preprocessing_function = applications.vgg16.preprocess_input)

nb_test_samples = 1

test_generator=datagen.flow_from_dataframe(dataframe=test_data, directory=None, x_col="path", y_col="label", 
                                           class_mode="binary", target_size = (img_height, img_width))

eval_results = model_final.evaluate_generator(test_generator, steps = nb_test_samples)
accuracy = round(eval_results[1]*100, 2)
print(f"Predicted the diagnosis of {test_df.count()} patients with an accuracy of {accuracy}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize Predictions
# MAGIC 
# MAGIC Wow! That's incredible!!
# MAGIC 
# MAGIC Let's take a look at our predictions, and see which images we were most confident about.

# COMMAND ----------

from pyspark.sql.types import StringType
from keras.preprocessing import image
import numpy as np

# Define udf to generate predictions
@udf(StringType())
def predict_udf(image_data):
  path = image_data[0].replace("dbfs:", "/dbfs")
  img = image.load_img(path, target_size=(img_width, img_height))
  #convert to numpy array for Keras image processing
  x = image.img_to_array(img)
  x = np.expand_dims(x, axis=0)
  x = applications.vgg16.preprocess_input(x)
  preds = model_final.predict(x)
  if preds[0][0] > 0.5: # class of 1 = pneumonia
    return f"1: prob = {preds[0][0]}"
  return f"0: prob = {1-preds[0][0]}"

pred_df = test_df.withColumn("predicted", predict_udf("image"))
display(pred_df)

# COMMAND ----------

# # pandas udf version
# from pyspark.sql.functions import pandas_udf

# from pyspark.sql.types import StringType
# from keras.preprocessing import image
# import numpy as np
# import pandas as pd

# # Define udf to generate predictions
# @pandas_udf(StringType())
# def pandas_predict_udf(image_data):
#   predictions = []
#   image_paths = image_data["origin"]
#   for image_path in image_paths:
#     path = image_path.replace("dbfs:", "/dbfs")
#     img = image.load_img(path, target_size=(img_width, img_height))
#     # convert to numpy array for Keras image processing
#     x = image.img_to_array(img)
#     x = np.expand_dims(x, axis=0)
#     x = applications.vgg16.preprocess_input(x)
#     preds = model_final.predict(x)
#     if preds[0][0] > 0.5: # class of 1 = pneumonia
#       predictions.append(f"1: prob = {preds[0][0]}")
#     else:
#       predictions.append(f"0: prob = {1-preds[0][0]}")
#   return pd.Series(predictions)

# pandas_udf_pred_df = pred_df.withColumn("pandas udf predicted", pandas_predict_udf("image"))
# display(pandas_udf_pred_df)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>