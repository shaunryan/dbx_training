# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Best Practices

# COMMAND ----------

# MAGIC %md
# MAGIC [**Scaling Deep Learning Best Practices**](https://databricks-prod-cloudfront.cloud.databricks.com/public/793177bc53e528530b06c78a4fa0e086/0/14560762/100107/latest.html)
# MAGIC * Use a GPU
# MAGIC * Early Stopping
# MAGIC * Larger batch size + learning rate
# MAGIC * Use /dbfs/ml and Petastorm
# MAGIC * Use Multiple GPUs with Horovod

# COMMAND ----------

# MAGIC %md
# MAGIC [**ULMFiT - Language Model Fine-tuning**](https://arxiv.org/pdf/1801.06146.pdf)
# MAGIC 
# MAGIC * Discriminative Fine Tuning: Different LR per layer
# MAGIC * Slanted triangular learning rates: Linearly increase learning rate, followed by linear decrease in learning rate
# MAGIC * Gradual Unfreezing: Unfreeze last layer and train for one epoch and keep unfreezing layers until all layers trained/terminal layer

# COMMAND ----------

# MAGIC %md
# MAGIC [**Bag of Tricks for CNN**](https://arxiv.org/pdf/1812.01187.pdf)
# MAGIC * Use Xavier Initalization
# MAGIC * Learning rate warmup (start with low LR and change to a higher LR)
# MAGIC * Increase learning rate for larger batch size
# MAGIC * No regularization on bias/no weight decay for bias terms
# MAGIC * Knowledge Distillation: Use a more complex model to train a smaller model by adjusting the loss to include difference in softmax values between the more accurate and smaller model 
# MAGIC * Label Smoothing: Adjust labels so that softmax output will have probability 1 - ε for the correct class and ε/(K − 1) for the incorrect class, K is the number of labels
# MAGIC * Image Augmentation:
# MAGIC   * Random crops of rectangular areas in image
# MAGIC   * Random flips 
# MAGIC   * Adjust hue, saturation, brightness
# MAGIC   * Add PCA noise with a coefficient sampled from a normal distribution

# COMMAND ----------

# MAGIC %md
# MAGIC [**fast.ai best practices**](https://forums.fast.ai/t/30-best-practices/12344)
# MAGIC * Do as much of your work as you can on a small sample of the data
# MAGIC * Batch normalization works best when done after ReLU
# MAGIC * Data Augmentation: Use the right kind of augmentation (e.g. don't flip a cat upside down, but satellite image OK)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>