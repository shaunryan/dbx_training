# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab: Drift Monitoring Algorithms
# MAGIC There are a many different algorithms for detecting drift, each with their own applicaitons.  Compare two the main drift detection algorithms to see how they perform on gradual and abrupt drift.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lab you:<br>
# MAGIC  - Create a two dummy datasets for gradual and sudden drift
# MAGIC  - Use <a href="https://scikit-multiflow.github.io/scikit-multiflow/skmultiflow.drift_detection.html#module-skmultiflow.drift_detection" target="_blank">the package `skmultiflow`</a> for comparing the DDM and EDDM algorithms
# MAGIC  

# COMMAND ----------

# MAGIC %run "./../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating the Data
# MAGIC 
# MAGIC The EDDM algorithm looks to improve performance on gradual drift while maintaining DDM's strong performance on abrupt drift.  In this lab, compare the two to see how they compare.
# MAGIC 
# MAGIC Create two datasets, one with abrupt drift and one with gradual drift.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Get a sense for how `numpy` creates a random sample from the binomial distribution.  It takes three parameters: number of trials, probability of success, and size of the output.  Adjust `p`, the second parameter, to change how abrupt our drift is.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> <a href="https://docs.scipy.org/doc/numpy/reference/generated/numpy.random.binomial.html#numpy.random.binomial" target="_blank">See the docs here.</a>

# COMMAND ----------

import numpy as np

data_points = 10
np.random.binomial(1, 0, data_points)

# COMMAND ----------

np.random.binomial(1, 1, data_points)

# COMMAND ----------

np.random.binomial(1, .5, data_points)

# COMMAND ----------

# MAGIC %md
# MAGIC Now create a dataset with gradual drift.

# COMMAND ----------

gradual_drift = []

for i in range(1000):
  gradual_drift.append(np.random.binomial(1, i/1000., 1)[0])

# COMMAND ----------

# MAGIC %md
# MAGIC Also create a dataset with sudden drift.

# COMMAND ----------

sudden_drift = np.random.binomial(1, .2, 1000).tolist()

for i in range(499, 1000):
    sudden_drift[i] = 1

# COMMAND ----------

# MAGIC %md
# MAGIC Visualize the two datasets.

# COMMAND ----------

import matplotlib.pyplot as plt

fig, ax = plt.subplots()

plt.subplot(2, 1, 1)
plt.scatter(range(len(sudden_drift)), sudden_drift, alpha=.1)
plt.title("Sudden Drift")

plt.subplot(2, 1, 2)
plt.scatter(range(len(gradual_drift)), gradual_drift, alpha=.1)
plt.title("Gradual Drift")

display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare the DDM and EDDM
# MAGIC 
# MAGIC Compare the two algorithms on how they detect gradual vs sudden drift.

# COMMAND ----------

from skmultiflow.drift_detection.ddm import DDM
from skmultiflow.drift_detection.eddm import EDDM

eddm = EDDM()
ddm = DDM()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Start with the gradual drift dataset.  For each data point, add it to both `eddm` and `ddm` and then print out the index where it detects a warning zone and where it detects change.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> <a href="https://scikit-multiflow.github.io/scikit-multiflow/skmultiflow.drift_detection.html#module-skmultiflow.drift_detection" target="_blank">See the `skmultiflow` docs for details.</a>

# COMMAND ----------

# ANSWER
for i, g in enumerate(gradual_drift):
  ddm.add_element(g)
  eddm.add_element(g)
  if ddm.detected_warning_zone():
     print("Warning zone detected by DDM at index {} data {}".format(i, g))
  if ddm.detected_change():
     print("Change detected by DDM at index {}".format(i))
      
  if eddm.detected_warning_zone():
     print("Warning zone detected by EDDM at index {} data {}".format(i, g))
  if eddm.detected_change():
     print("Change detected by EDDM at index {}".format(i))

# COMMAND ----------

# MAGIC %md
# MAGIC Now do the same for sudden drift.

# COMMAND ----------

# ANSWER
for i, s in enumerate(sudden_drift):
  ddm.add_element(s)
  eddm.add_element(s)
  if ddm.detected_warning_zone():
     print("Warning zone detected by DDM at index {} data {}".format(i, s))
  if ddm.detected_change():
     print("Change detected by DDM at index {}".format(i))
      
  if eddm.detected_warning_zone():
     print("Warning zone detected by EDDM at index {} data {}".format(i, s))
  if eddm.detected_change():
     print("Change detected by EDDM at index {}".format(i))

# COMMAND ----------

# MAGIC %md
# MAGIC Which performed better?
# MAGIC 
# MAGIC Try changing the parameters for the two drift direction algorithms and observe the results.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Next Lesson<br>
# MAGIC 
# MAGIC ### Start the next lesson, [Alerting]($../07-Alerting )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>