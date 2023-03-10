# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Drift Monitoring
# MAGIC 
# MAGIC Monitoring models over time entails safeguarding against drift in model performance.  In this lesson, you explore solutions to drift and implement a basic retraining method and two ways of dynamically monitoring drift.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Analyze model monitoring strategies for data drift and model drift
# MAGIC  - Apply a non-adaptive drift solution that periodically retrains a model
# MAGIC  - Apply an adaptive solution that detects model drift using an error threshold 
# MAGIC  - Apply an adaptive solution that detects model drift using more complex error statistics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drift
# MAGIC 
# MAGIC The majority of machine learning solutions...<br><br>
# MAGIC 
# MAGIC * Assume that data is generated according to a stationary probability distribution
# MAGIC * Most datasets involving human activity evolve over time
# MAGIC * A **context** is a set of samples from a distribution that are generated by a stationary function
# MAGIC * When this context changes, we can observe a change in the probability distribution
# MAGIC 
# MAGIC **Monitoring for drift involves monitoring for this change of context, often quantified by an increase in the error rate.**

# COMMAND ----------

# MAGIC %md
# MAGIC There are three main types of change to worry about in machine learning:
# MAGIC 
# MAGIC 1. **Concept drift** 
# MAGIC   - The statistical properties of the _target variable_ change
# MAGIC   - Example: a new fraud strategy you haven't seen in the past
# MAGIC 2. **Data drift**
# MAGIC   - The statistical properties of the _input variables_ change
# MAGIC   - Example: seasonality of the data or changes in personal preferences
# MAGIC 3. **Upstream data changes** 
# MAGIC   - Encoding of features change or missing values
# MAGIC   - Example: Fahrenheit is now encoded as Celsius

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC There are two basic categories of solutions...<br><br>
# MAGIC 
# MAGIC * The first adapts the learner regardless of whether drift has occurred
# MAGIC   - This solution would include some notion of memory
# MAGIC   - Filtering for a certain time window (e.g. all data from the past week) 
# MAGIC   - Using weighted examples (e.g. this month's data is weighted twice as important as last month's)
# MAGIC   - The main challenge with this option is choosing the best window or filtering method
# MAGIC * Without the ability to actively detect drift, it somewhat arbitrarily selects that threshold
# MAGIC 
# MAGIC The second solution adapts the learner when it detects drift...<br><br>  
# MAGIC 
# MAGIC * An adaptive solution is ideal since it can detect the optimal time for retraining 
# MAGIC * Will make more data available to the learner, improving model performance

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/drift.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 
# MAGIC This is an example of an adaptive learner...<br><br>  
# MAGIC 
# MAGIC * It monitors incoming data coming incrementally or in new batches for an increase in error.
# MAGIC * Once that error reaches a certain threshold, a warning is issued.
# MAGIC * Once it reaches a second threshold, the data is said to be in a new context.
# MAGIC   - This triggers a retraining of the model on the data between the first and second thresholds.
# MAGIC * The thresholds in this example are set using the 95% and 99% confidence intervals.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basic Retraining Solution
# MAGIC 
# MAGIC Basic solutions to the issue of drift can include the following:<br><br>
# MAGIC 
# MAGIC  - Retrain the model periodically on all new and historical data
# MAGIC  - Retrain the model on a known window of data (e.g. the last week of data)
# MAGIC  - Retrain the model while weighing more recent data more strongly
# MAGIC 
# MAGIC To illustrate model retraining, look at code that changes over time to get a sense for how it affects model error.

# COMMAND ----------

# MAGIC %md
# MAGIC Set up a series of predictions drawn from 3 different distributions.

# COMMAND ----------

import numpy as np
import pandas as pd

np.random.seed(42)

distribution1 = np.stack([np.random.normal(loc=0, scale=1, size=1000), np.random.random(1000), np.ones(1000)*1]).T
distribution2 = np.stack([np.random.normal(loc=2, scale=1.5, size=1000), np.random.random(1000), np.ones(1000)*2]).T
distribution3 = np.stack([np.random.normal(loc=5, scale=1, size=1000), np.random.random(1000), np.ones(1000)*3]).T

df = pd.DataFrame(np.vstack([distribution1, distribution2, distribution3]), columns=["y", "random-x", "distribution"])
df["time"] = df.index

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC Visualize the changing distribution over time.

# COMMAND ----------

import matplotlib.pyplot as plt

fig, ax = plt.subplots()

ax.scatter(df['time'], df['y'], c=df['distribution'], alpha=.2)
ax.set_title("Changing Distribution over Time")
ax.set_xlabel("Time")
ax.set_ylabel("Value")

display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC We can see that the average and spread of the data changes over time across our three distributions.  Write a helper function to train models on subsets of the data.

# COMMAND ----------

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

def return_mse(df, distribution=None):
  if distribution:
    subset_df = df[df['distribution'] == distribution]
  else:
    subset_df = df.copy()
    
  trained_model = RandomForestRegressor(n_estimators=100, random_state=42).fit(subset_df['random-x'].values.reshape(-1, 1), subset_df['y'])
  mse = mean_squared_error(subset_df['y'], trained_model.predict(subset_df['random-x'].values.reshape(-1, 1)))
  return mse, trained_model
  
return_mse(df)[0]

# COMMAND ----------

# MAGIC %md
# MAGIC See how the different subsets compare.

# COMMAND ----------

print("MSE for model trained on all data: {}".format(return_mse(df)[0]))
print("MSE for model trained on distribution 1: {}".format(return_mse(df, 1)[0]))
print("MSE for model trained on distribution 2: {}".format(return_mse(df, 2)[0]))
print("MSE for model trained on distribution 3: {}".format(return_mse(df, 3)[0]))

# COMMAND ----------

# MAGIC %md
# MAGIC What is the optimal window to retrain the model?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detecting by Error Threshold
# MAGIC 
# MAGIC Monitoring for error threshold is a more dynamic way of handling model drift.  In practice, this is one of the more common ways of handling drift where a data scientist is alerted at the point the model error rises above a certain level.
# MAGIC 
# MAGIC Create a process for alerting when an error rate increases above 0.5.

# COMMAND ----------

# MAGIC %md
# MAGIC Create the error threshold.

# COMMAND ----------

mse, model = return_mse(df, 1)
mse_threshold = mse * 1.5

# COMMAND ----------

# MAGIC %md
# MAGIC Create a helper function to test a model.

# COMMAND ----------

def test_for_drift(df, threshold, distribution=None):
  if return_mse(df, distribution)[0] > threshold:
    return "The model has drifted"
  else:
    return "The model has not drifted"

# COMMAND ----------

# MAGIC %md
# MAGIC Test the first distribution.

# COMMAND ----------

test_for_drift(df, mse_threshold, distribution=1)

# COMMAND ----------

# MAGIC %md
# MAGIC Test the next distribution.

# COMMAND ----------

test_for_drift(df, mse_threshold, distribution=2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detecting Drift by Statistical Distribution
# MAGIC 
# MAGIC As we saw above, choosing the best window to retrain our model is difficult, and is often done somewhat randomly.  The best window depends on a number of issues, especially how abruptly we expect drift to happen.
# MAGIC 
# MAGIC A more rigorous way of addressing drift is to first detect drift using statistical methods.  This could be as simple as monitoring model error.  The package <a href="https://scikit-multiflow.github.io/scikit-multiflow/skmultiflow.drift_detection.html#module-skmultiflow.drift_detection" target="_blank">`skmultiflow` has some good options for this.</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Try the DDM method.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/drift.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 
# MAGIC The detection threshold is calculated as a function of two statistics, obtained when `(pi + si)` is minimum:
# MAGIC 
# MAGIC  * `pmin`: The minimum recorded error rate
# MAGIC  * `smin`: The minimum recorded standard deviation
# MAGIC 
# MAGIC At instant `i`, the detection algorithm uses:
# MAGIC 
# MAGIC  * `pi`: The error rate at instant i
# MAGIC  * `si`: The standard deviation at instant i
# MAGIC 
# MAGIC The default conditions for entering the warning zone and detecting change are as follows:
# MAGIC 
# MAGIC  * if `pi + si >= pmin + 2 * smin` -> Warning zone
# MAGIC  * if `pi + si >= pmin + 3 * smin` -> Change detected
# MAGIC  
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> <a href="https://github.com/scikit-multiflow/scikit-multiflow/blob/ddf104437132f3b75f3fa6d30195e09d7bcb3231/src/skmultiflow/drift_detection/ddm.py#L5" target="_blank">See the implementation here.</a>

# COMMAND ----------

# MAGIC %md
# MAGIC The Python (PyPi) library `scikit-multiflow` is required for this lesson.
# MAGIC   - This allows us to manipulate Azure ML programatically.
# MAGIC   - <a href="https://files.training.databricks.com/static/step-by-step/installing-libraries-from-pypi.html" target="_blank">See the instructions on how to install a library from PyPi</a> if you're unfamiliar with the process.

# COMMAND ----------

# MAGIC %md
# MAGIC Define the DDM object.  Set the parameters for the minimum number of instances before alerting a change as well as for the two thresholds.

# COMMAND ----------

import numpy as np
from skmultiflow.drift_detection import DDM

ddm = DDM(min_num_instances=100, warning_level=1.98, out_control_level=2)

# COMMAND ----------

# MAGIC %md
# MAGIC Simulate a data stream as a uniform distribution of 1's and 0's.  These numbers represent misclassification error where a 1 is a misclassified prediction.

# COMMAND ----------

data_points=8000
data_stream = np.random.randint(2, size=data_points)

# COMMAND ----------

# MAGIC %md
# MAGIC Simulate a change in context by making the algorithm perform poorly at different points.

# COMMAND ----------

for i in range(999, 1500):
    data_stream[i] = 1

for i in range(3500, 6500):
    data_stream[i] = 1

for i in range(7500, 8000):
    data_stream[i] = 1

# COMMAND ----------

# MAGIC %md
# MAGIC Now run the simulated errors through DDM.  Alert if a warning zone or change zone has been detected.

# COMMAND ----------

for i in range(data_points):
    ddm.add_element(data_stream[i])
    if ddm.detected_warning_zone():
       print("Warning zone detected at index {} data {}".format(i, data_stream[i]))
    if ddm.detected_change():
       print("Change detected at index {}".format(i))
#        ddm.reset()

# COMMAND ----------

# MAGIC %md
# MAGIC Did the algorithm detect change where you thought it would?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC 
# MAGIC **Question:** Why do some of the common assumptions of machine learning not apply to real world datasets?  
# MAGIC **Answer:** One major assumption in machine learning is the existence of static data created by a static distribution.  The reality is that data changes over time when the underlying mechanism of that change is not fully modeled.  Real world machine learning models have to be able to handle "concept drift" where the context in which the model was changed evolves over time.  This can pose many challenges, one being in recommender systems that want to recommend to different modalities of a user.  For instance, one of those modalities might be nostalgia, which would require the use of older data.
# MAGIC 
# MAGIC **Question:** What are the pros and cons of using a window or function that considers recent data more strongly?  
# MAGIC **Answer:** The main benefit of this approach is that it will adapt a model over time.  The downside is that it is difficult to know how to set the parameters of that window.  We know with statistical certainty that models perform better with more data when that data originates from the same probability distribution.  However, knowing when that context has changed is challenging, so knowing what data to use when retraining a model is non-trivial.
# MAGIC 
# MAGIC **Question:** What is the best way of knowing when to retrain a model?  
# MAGIC **Answer:** The easiest solution is to set up an alerting mechanism for when a model's error begins to slip.  The naive implementation of this might look for a 10% slip in error before raising an alarm.  A more rigorous approach would look to quantify the shift in context.  These approaches often look at the confidence intervals for a given outcome such as the label.  When a threshold is reached, it is said that the data is originating from a new context.  These adaptive solutions are largely understudied, so academic research and newer libraries are the best place to source these solutions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Lab<br>
# MAGIC 
# MAGIC ### [Click here to start the lab for this lesson.]($./Labs/06-Lab) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** What are other examples of handling drift?  
# MAGIC **A:** Check out <a href="https://github.com/joelcthomas/modeldrift" target="_blank">this end-to-end example</a> that provides an example of data drift and provides a dynamic solution for model retraining.
# MAGIC 
# MAGIC **Q:** What are libraries for handling drift?  
# MAGIC **A:** Check out <a href="https://moa.cms.waikato.ac.nz/" target="_blank">MOA</a>, <a href="https://scikit-multiflow.github.io/" target="_blank">scikit-multiflow</a> and <a href="https://www.tensorflow.org/tfx/data_validation/get_started" target="_blank">TensorFlow Data Validation.</a>
# MAGIC 
# MAGIC **Q:** What's a good general introduction to drift?  
# MAGIC **A:** Check out the <a href="https://en.wikipedia.org/wiki/Concept_drift" target="_blank">Wikipedia article</a> on drift.
# MAGIC 
# MAGIC **Q:** What are good academic resources for drift?  
# MAGIC **A:** Check out the papers <a href="http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.108.1833&rep=rep1&type=pdf" target="_blank">Learning with Drift Detection</a>, <a href="https://www.win.tue.nl/~mpechen/publications/pubs/CD_applications15.pdf" target="_blank">An overview of concept drift applications</a>, and <a href="https://arxiv.org/pdf/1504.01044.pdf" target="_blank">Concept Drift Detection for Streaming Data</a>.
# MAGIC 
# MAGIC **Q:** How can I classify different types of drift?  
# MAGIC **A:** Check out the paper <a href="https://rtg.cis.upenn.edu/cis700-2019/papers/dataset-shift/dataset-shift-terminology.pdf" target="_blank">A unifying view on dataset shift in classification</a>.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>