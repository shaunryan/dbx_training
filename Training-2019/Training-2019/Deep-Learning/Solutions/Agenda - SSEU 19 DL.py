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
# MAGIC | 30m  | **[Linear Regression]($./DL 01 - Linear Regression)** | - Build a linear regression model using Sklearn and reimplement it in Keras <br/>- Modify # of epochs <br/>- Visualize loss | 
# MAGIC | 30m  | **Break (10:30 - 11:00)**                                               ||
# MAGIC | 30m  | **[Keras]($./DL 02 - Keras)**  | Modify these parameters for increased model performance:<br/> - Activation functions<br/>- Loss functions<br/>- Optimizer<br/>- Batch Size |
# MAGIC | 30m    | **[Keras Lab]($./Labs/DL 02L - Keras Lab)**    | Build and evaluate your first Keras model! <br/> (Students use Boston Housing Dataset, Instructor uses California Housing) |
# MAGIC | 35m  | **[Advanced Keras]($./DL 03 - Advanced Keras)**       | - Perform data standardization for better model convergence<br/> - Create custom metrics<br/> - Add validation data<br/> - Generate model checkpointing/callbacks<br/> - Save and load models |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 1 PM
# MAGIC | Time | Lesson &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 30m  | **[Advanced Keras Lab]($./Labs/DL 03L - Advanced Keras Lab)**      | - Perform data standardization<br/>- Generate a separate train/validation dataset<br/> - Create earlycheckpointing callback<br/> - Load and apply your saved model|
# MAGIC | 30m |**[MLflow]($./DL 04 - MLflow)** | - Log experiments with MLflow<br/> - View MLflow UI<br/> - Generate a UDF with MLflow and apply to a Spark DataFrame |
# MAGIC | 20m  | **[MLflow Lab]($./Labs/DL 04L - MLflow Lab)** | - Add MLflow to your experiments from the Boston Housing Dataset!<br/>**Bonus:**<br/> Create LambdaCallback to log MLflow metrics while the model is training (after each epoch)<br/> Create a UDF that you can invoke in SQL <br/> Get the lowest MSE! | 
# MAGIC | 30m  | **Break (15:00 - 15:30)**                                               ||
# MAGIC | 40m | **[Horovod]($./DL 05 - Horovod)** |* - Use Horovod to train a distributed neural network</br> - Distributed Deep Learning best practices* |
# MAGIC | 25m    | **[Horovod Petastorm]($./DL 05a - Horovod Petastorm)**    | * - Use Horovod to train a distributed neural network using Parquet files + Petastorm* |
# MAGIC | 30m  | **[Model Interpretability]($./DL 06 - Model Interpretability)**      | * - Use LIME and SHAP to understand which features are most important in the model's prediction for that data point* |

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>