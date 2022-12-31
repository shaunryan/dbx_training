# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Agenda
# MAGIC 
# MAGIC This is a two-day course covering the fundamentals of Deep Learning through Distributed Deep Learning & Best Practices.
# MAGIC 
# MAGIC **Cluster Requirements:**
# MAGIC * 5.5 LTS ML 
# MAGIC 
# MAGIC **Libraries (PyPI):**
# MAGIC * shap==0.29.2
# MAGIC * scikit-image==0.15.0
# MAGIC * lime==0.1.1.36
# MAGIC * petastorm==0.7.5
# MAGIC * mlflow==1.2.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 1 AM
# MAGIC | Time | Lesson &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 30m  | **Introductions & Setup**                               | *Registration, Courseware & Q&As* |
# MAGIC | 30m    | **[Spark Review]($./DL 00 - Intro to Spark)**    | - Create a Spark DataFrame<br/> - Analyze the Spark UI <br/>- Cache data <br/>- Change Spark default configurations to speed up queries |
# MAGIC | 10m  | **Break**                                               ||
# MAGIC | 35m  | **[Linear Regression]($./DL 01 - Linear Regression)** | - Build a linear regression model using Sklearn and reimplement it in Keras <br/>- Modify # of epochs <br/>- Visualize loss | 
# MAGIC | 30m  | **[Keras]($./DL 02 - Keras)**  | Modify these parameters for increased model performance:<br/> - Activation functions<br/>- Loss functions<br/>- Optimizer<br/>- Batch Size |
# MAGIC | 10m  | **Break**                                               ||
# MAGIC | 30m    | **[Keras Lab]($./Labs/DL 02L - Keras Lab)**    | Build and evaluate your first Keras model! <br/> (Students use Boston Housing Dataset, Instructor uses California Housing) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 1 PM
# MAGIC | Time | Lesson &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 35m  | **[Advanced Keras]($./DL 03 - Advanced Keras)**       | - Perform data standardization for better model convergence<br/> - Create custom metrics<br/> - Add validation data<br/> - Generate model checkpointing/callbacks<br/> - Save and load models |
# MAGIC | 30m  | **[Advanced Keras Lab]($./Labs/DL 03L - Advanced Keras Lab)**      | - Perform data standardization<br/>- Generate a separate train/validation dataset<br/> - Create earlycheckpointing callback<br/> - Load and apply your saved model|
# MAGIC | 10m  | **Break**                                               ||
# MAGIC | 30m |**[MLflow]($./DL 04 - MLflow)** | - Log experiments with MLflow<br/> - View MLflow UI<br/> - Generate a UDF with MLflow and apply to a Spark DataFrame |
# MAGIC | 20m  | **[MLflow Lab]($./Labs/DL 04L - MLflow Lab)** | - Add MLflow to your experiments from the Boston Housing Dataset!<br/>**Bonus:**<br/> Create LambdaCallback to log MLflow metrics while the model is training (after each epoch)<br/> Create a UDF that you can invoke in SQL <br/> Get the lowest MSE! | 
# MAGIC | 10m  | **Break**                                               ||
# MAGIC | 40m | **[Horovod]($./DL 05 - Horovod)** |* - Use Horovod to train a distributed neural network</br> - Distributed Deep Learning best practices* |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 2 AM
# MAGIC | Time | Lesson &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 20m  | **Review**                               | *Review of Day 1* |
# MAGIC | 30m    | **[Horovod Petastorm]($./DL 05a - Horovod Petastorm)**    | * - Use Horovod to train a distributed neural network using Parquet files + Petastorm* |
# MAGIC | 10m  | **Break**                                               ||
# MAGIC | 40m  | **[Horovod ALS]($./DL 05b - Horovod ALS)**  | * - Combine User + Item factors identified from ALS and use as input to a neural network<br/>- Create custom activation function (scaled sigmoid) to bound output of regression tasks<br/> - Train distributed neural network using Horovod* |
# MAGIC | 45m  | **[Horovod Lab]($./Labs/DL 05L - Horovod Lab)**  | *  - Prepare your data for use with Horovod</br> - Distribute the training of our model using HorovodRunner</br> - Use Parquet files as input data for our distributed deep learning model with Petastorm + Horovod* |
# MAGIC | 10m  | **Break**                                               ||
# MAGIC | 35m  | **[Model Interpretability]($./DL 06 - Model Interpretability)**      | * - Use LIME and SHAP to understand which features are most important in the model's prediction for that data point* |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 2 PM
# MAGIC | Time | Lesson &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 45m  | **[CNNs]($./DL 07 - CNNs)** | - Analyze popular CNN architectures<br/>- Apply pre-trained CNNs to images using Pandas Scalar Iterator UDF | 
# MAGIC | 20m |**[Lime for CNNs]($./DL 07a - Lime for CNNs)** | * - Use LIME to visualize how the CNN makes predictions* |
# MAGIC | 10m  | **Break**                                               ||
# MAGIC | 30m  | **[Transfer Learning]($./DL 08 - Transfer Learning)**       | * - Perform transfer learning to create a cat vs dog classifier* |
# MAGIC | 20m | **[Transfer Learning Lab]($./Labs/DL 08L - Transfer Learning Lab)** |* - Build a model with nearly perfect accuracy predicting if a patient has pneumonia or not using transfer learning* |
# MAGIC | 10m  | **Break**                                               ||
# MAGIC | 20m |**[HyperOpt]($./DL 09 - HyperOpt)** | * - Use HyperOpt to train and optimize a feed-forward neural net* |
# MAGIC | 25m | **[Best Practices]($./DL 10 - Best Practices)** |* - Discuss DL best practices, state of the art, and new research areas*|

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>