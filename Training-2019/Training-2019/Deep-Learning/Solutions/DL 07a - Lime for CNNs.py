# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # LIME for CNNs
# MAGIC 
# MAGIC 
# MAGIC We will use the same VGG16 pre-trained Convolutional Neural Networks (CNNs), trained with the image dataset from [ImageNet](http://www.image-net.org/), and Lime to see which part of the image contributes to the label. 
# MAGIC 
# MAGIC **Required Libraries (PyPI)**:
# MAGIC * scikit-image==0.15.0
# MAGIC * lime==0.1.1.36
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Use LIME to understand how the CNN model makes a prediction

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

from tensorflow.keras.preprocessing import image
from tensorflow.keras.applications.vgg16 import preprocess_input, decode_predictions, VGG16
import numpy as np
import os
import matplotlib.pyplot as plt
from skimage.segmentation import mark_boundaries

vgg16Model = VGG16(weights='imagenet')

def predict_images(images, model):
  for i in images:
    print(f"processing image: {i}")
    img = image.load_img(i, target_size=(224, 224))
    #convert to numpy array for Keras image formate processing
    x = image.img_to_array(img)
    x = np.expand_dims(x, axis=0)
    x = preprocess_input(x)
    preds = model.predict(x)
    # decode the results into a list of tuples (class, description, probability
    print("Predicted: ", decode_predictions(preds, top=3)[0], "\n")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Images
# MAGIC <div style="text-align: left; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://brookewenig.github.io/img/DL/pug.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC   <img src="https://brookewenig.github.io/img/DL/strawberries.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC   <img src="https://brookewenig.github.io/img/DL/rose.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC   
# MAGIC </div>
# MAGIC 
# MAGIC Let's output the top 3 predictions for 3 images.

# COMMAND ----------

img_paths = ["/dbfs/mnt/training/dl/img/pug.jpg", "/dbfs/mnt/training/dl/img/strawberries.jpg", "/dbfs/mnt/training/dl/img/rose.jpg"]
predict_images(img_paths, vgg16Model)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we create a function to process the image to be in the correct format for the Lime model interpreter, We can apply it to the image of a pug.

# COMMAND ----------

def transform_img_fn(path_list):
    out = []
    for img_path in path_list:
        img = image.load_img(img_path, target_size=(224, 224))
        x = image.img_to_array(img)
        x = np.expand_dims(x, axis=0)
        x = preprocess_input(x)
        out.append(x)
    return np.vstack(out)
  
pug = transform_img_fn(["/dbfs/mnt/training/dl/img/pug.jpg"])[0]

# COMMAND ----------

# MAGIC %md
# MAGIC We can run `LimeImageExplainer` to determine which parts of the image led the model to predict that the image is a picture of a pug.

# COMMAND ----------

from lime import lime_image
explainer = lime_image.LimeImageExplainer()
explanation = explainer.explain_instance(pug, vgg16Model.predict, top_labels=900, hide_color=0, num_samples=80)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can visualize the explanation by seeing which parts of the image caused the prediction. The green coloring indicates those pixels contributed to the predicted class.

# COMMAND ----------

# First Argument is index of category eg. pug is 254, see https://gist.github.com/yrevar/942d3a0ac09ec9e5eb3a
pug_index = 254
temp, mask = explanation.get_image_and_mask(pug_index, positive_only=False, num_features=20, hide_rest=True, min_weight=0)
fig, ax = plt.subplots()
plt.imshow(mark_boundaries(temp, mask))
display(fig)

# COMMAND ----------

pug_resized = image.load_img("/dbfs/mnt/training/dl/img/pug.jpg", target_size=(224, 224))
_, mask = explanation.get_image_and_mask(pug_index, positive_only=False, num_features=20, hide_rest=True, min_weight=0)
plt.imshow(mark_boundaries(pug_resized, mask))
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can use Lime on an image of a rose and determine what caused it to predict sea anemone.

# COMMAND ----------

rose = transform_img_fn(["/dbfs/mnt/training/dl/img/rose.jpg"])[0]
explainer = lime_image.LimeImageExplainer()
explanation = explainer.explain_instance(rose, vgg16Model.predict, top_labels=900, hide_color=0, num_samples=50)#Recommend using more samples, 6 samples leads to almost entirely black output

# COMMAND ----------

sea_anemone_index = 108
temp, mask = explanation.get_image_and_mask(sea_anemone_index, positive_only=False, num_features=20, hide_rest=True) #Toggle hide_rest to display rest of image overlapped
plt.imshow(mark_boundaries(temp, mask))
display(fig)

# COMMAND ----------

rose_resized = image.load_img("/dbfs/mnt/training/dl/img/rose.jpg", target_size=(224, 224))
temp, mask = explanation.get_image_and_mask(sea_anemone_index, positive_only=False, num_features=20, hide_rest=False, min_weight=0)
plt.imshow(mark_boundaries(rose_resized, mask))
display(fig)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>