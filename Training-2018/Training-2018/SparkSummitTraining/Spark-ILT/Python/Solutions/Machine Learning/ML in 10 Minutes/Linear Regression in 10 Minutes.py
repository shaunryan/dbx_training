# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Linear Regression
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Introduction
# MAGIC 
# MAGIC Linear Regression is a widely used basic Supervised Machine Learning algorithm. It attempts to model the relationship between our label and one or more dependent variables by fitting a linear equation to the observed data. The Linear Regression Model explains which variables are significant predictors of the label. It does this by assigning weights (coefficients) to each feature in the model.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/linear-regression.png">
# MAGIC 
# MAGIC <small>Dataset &amp; Figure: https://en.wikipedia.org/wiki/Anscombe%27s_quartet</small>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Documentation

# COMMAND ----------

# MAGIC %md
# MAGIC * <a href='https://spark.apache.org/docs/latest/ml-classification-regression.html#linear-regression' target="_blank">Linear Regression documentation</a> in the Spark ML programming guide

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Linear Regression in Action

# COMMAND ----------

# MAGIC %md
# MAGIC Let's see our data on a Scatter plot:

# COMMAND ----------

from pyspark.sql import Row

d = [
  Row(4.0, 4.26),
  Row(5.0, 5.68),
  Row(6.0, 7.24),
  Row(7.0, 4.82),
  Row(8.0, 6.95),
  Row(9.0, 8.81),
  Row(10.0, 8.04),
  Row(11.0, 8.33),
  Row(12.0, 10.84),
  Row(13.0, 7.58),
  Row(14.0, 9.96)
]

df = spark.createDataFrame(d,["x","y"])
df.createOrReplaceTempView("points")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Now, import the *Linear Regression* class and take a look at the its parameters:

# COMMAND ----------

from pyspark.ml.regression import LinearRegression
lr = LinearRegression()

print(lr.explainParams())

# COMMAND ----------

# MAGIC %md
# MAGIC We are using a <a href="https://spark.apache.org/docs/latest/ml-features.html#vectorassembler" target="_blank">VectorAssembler</a> to create a Vector from the *x* column and then we are training our model:

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(inputCols=["x"], outputCol="features")
vecDF = assembler.transform(df)

lr.setLabelCol("y")
lrModel = lr.fit(vecDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Our model is a linear equation in the form of `y = a + b*x`, where `a` is called the *intercept* and *b* is called the coefficient.
# MAGIC 
# MAGIC *If you have n dependent variables you will have n coefficients* and the equation will take the form of `y = a + b1*x1 + b2*c2 + ... + bn*xn`.
# MAGIC 
# MAGIC Print the equation to the screen:

# COMMAND ----------

intercept = lrModel.intercept
coefficient = lrModel.coefficients[0]

print("Linear Equation: y = {:.2f} + {:.2f} * x".format(intercept, coefficient))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC That's exactly the blue line in our figure displayed above. 

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at the point at `x=14`. Here `y` will be `3 + 0.5 * 14`, which will make it `y=10`.:

# COMMAND ----------

# MAGIC %python
# MAGIC import matplotlib.pyplot as plt
# MAGIC 
# MAGIC features, labels = zip(*spark.table("points").collect())
# MAGIC 
# MAGIC fig, ax = plt.subplots(figsize=(5,3))
# MAGIC ax.set_xlim(2.5,19)
# MAGIC ax.set_ylim(3,13)
# MAGIC ax.plot(features,labels,'o', markersize=11, color="orange")
# MAGIC ax.plot(range(2,20), list(map(lambda x: 3 + 0.5 * x, range(2,20))), markersize=0.1)
# MAGIC 
# MAGIC display(fig)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>