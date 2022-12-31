# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Transfer Learning
# MAGIC 
# MAGIC The idea behind transfer learning is to take knowledge from one model doing some task, and transfer it to build another model doing a similar task.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Perform transfer learning to create a cat vs dog classifier

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="text-align: left; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://files.training.databricks.com/images/mllib_dpl-1.jpg" height="500" width="900" alt="Deep Learning & Transfering Learning" style=>
# MAGIC </div>

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Notice how few images we have in our training data. Will our neural network be able to distinguish between cats and dogs?

# COMMAND ----------

from pyspark.sql.functions import lit
df_cats = spark.read.format("image").load("mnt/training/dl/img/cats/*.jpg").withColumn("label", lit("cat"))
df_dogs = spark.read.format("image").load("mnt/training/dl/img/dogs/*.jpg").withColumn("label", lit("dog"))
display(df_cats)

# COMMAND ----------

cat_data = df_cats.toPandas()
dog_data = df_dogs.toPandas()

train_data = cat_data.iloc[:5].append(dog_data.iloc[:5])
train_data["path"] = train_data["image"].apply(lambda x: x["origin"].replace("dbfs:/","/dbfs/"))

test_data = cat_data.iloc[5:].append(dog_data.iloc[5:])
test_data["path"] = test_data["image"].apply(lambda x: x["origin"].replace("dbfs:/","/dbfs/"))

print(f"Train data samples: {len(train_data)} \tTest data samples: {len(test_data)}")

# COMMAND ----------

from tensorflow.keras import applications
from tensorflow.keras.preprocessing.image import ImageDataGenerator
from tensorflow.keras.models import Model 
from tensorflow.keras.layers import Dense
from tensorflow import set_random_seed
np.random.seed(0)
set_random_seed(0)

nb_train_samples = 2
batch_size = 1
img_height = 224
img_width = 224

# Load original model with pretrained weights from imagenet
model = applications.VGG16(weights="imagenet", input_shape=(img_height, img_width, 3))

# Freeze all the original trained layers so we can reuse them
for layer in model.layers:
  layer.trainable = False
    
# Add custom new layer for binary cat/dog output
x = model.output
predictions = Dense(1, activation="sigmoid")(x)

model_final = Model(inputs=model.input, outputs=predictions)
model_final.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we only have to train 1,001 parameters instead of the 138,358,545 present in our network architecture.

# COMMAND ----------

model_final.compile(loss="binary_crossentropy", optimizer="adam", metrics=["accuracy"])

# Loading training data
train_datagen = ImageDataGenerator(preprocessing_function=applications.vgg16.preprocess_input)
train_generator = train_datagen.flow_from_dataframe(dataframe=train_data, directory=None, x_col="path", y_col="label", class_mode="binary", target_size = (img_height, img_width), batch_size = batch_size )

print(f"Class labels: {train_generator.class_indices}")

# Train the model 
model_final.fit_generator(train_generator, epochs=20, verbose=2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate the Accuracy

# COMMAND ----------

# Evaluate model on test set using Keras
test_datagen = ImageDataGenerator(preprocessing_function = applications.vgg16.preprocess_input)
nb_test_samples = 1
test_generator = test_datagen.flow_from_dataframe(dataframe=test_data, directory=None, x_col="path", y_col="label", class_mode="binary", target_size=(img_height, img_width))
eval_results = model_final.evaluate_generator(test_generator, steps=nb_test_samples)
print(f"Loss: {eval_results[0]}. Accuracy: {eval_results[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Transfer Learning
# MAGIC 
# MAGIC Can you think of any other techniques to improve our model?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>