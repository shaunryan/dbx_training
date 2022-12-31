// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # K-Nearest Neighbors
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Introduction
// MAGIC 
// MAGIC The k-nearest neighbors algorithm (kNN) is a method used for classification and regression. The input consists of the k closest training examples in the feature space. The output depends on whether k-NN is used for classification or regression:
// MAGIC  * In k-NN classification, the output is a class membership. An object is classified by a majority vote of its neighbors, with the object being assigned to the class most common among its k nearest neighbors. If k = 1, then the object is simply assigned to the class of that single nearest neighbor.
// MAGIC  * In k-NN regression, the output is the property value for the object. This value is the average of the values of its k nearest neighbors.
// MAGIC 
// MAGIC k-NN is a type of <a href="https://en.wikipedia.org/wiki/Instance-based_learning" target="_blank">instance-based learning</a>, or <a href="https://en.wikipedia.org/wiki/Lazy_learning" target="_blank">lazy learning</a>, where the function is only approximated locally and all computation is deferred until classification. 
// MAGIC   
// MAGIC The k-NN algorithm is among the simplest of all machine learning algorithms.
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC <small><i>Source: https://en.wikipedia.org/wiki/K-nearest_neighbors_algorithm</i></small>

// COMMAND ----------

// MAGIC %md
// MAGIC <h3>An example of a classification in the `K=1` case:</h3>
// MAGIC <img src="https://files.training.databricks.com/images/k-nearest-neighbors.png">

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Documentation

// COMMAND ----------

// MAGIC %md
// MAGIC * The <a href="http://scikit-learn.org/stable/modules/neighbors.html" target="_blank">scikit-learn's K-NN algorithm</a> can be used on the top of <a href="https://github.com/databricks/spark-sklearn">spark-sklearn</a> a library that integrates scikit-learn with Apache Spark.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) k-NN in Action

// COMMAND ----------

// MAGIC %md
// MAGIC **Let's generate and visualize our data**
// MAGIC 
// MAGIC We are creating a random dataset that contains 300 data points with 3 different labels *(0, 1 and 2)*

// COMMAND ----------

// MAGIC %python
// MAGIC import numpy as np
// MAGIC num_points = 100
// MAGIC sd = 13
// MAGIC np.random.seed(273)
// MAGIC 
// MAGIC points = np.concatenate((
// MAGIC   np.random.normal((105,100), (sd,sd), size=[num_points,2]),
// MAGIC   np.random.normal((40,100), (sd,sd),  size=[num_points,2]),
// MAGIC   np.random.normal((50,40),  (sd,sd),  size=[num_points,2])
// MAGIC   ))
// MAGIC 
// MAGIC label = [0]*num_points + [1]*num_points + [2]*num_points

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a look at the data

// COMMAND ----------

// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC plt.gcf().clear()
// MAGIC 
// MAGIC colors = ['r','b','g']
// MAGIC plt.scatter(points[:,0], points[:,1], c=[colors[i] for i in label], label=label, s=10, edgecolors='none')
// MAGIC plt.xlabel("x")
// MAGIC plt.ylabel("y")
// MAGIC 
// MAGIC display(plt.show())

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creating a k-NN classifier
// MAGIC 
// MAGIC We are using a single nearest neighbor classifier, `K=1`. We do this to help us understand the predictions through a simple visualisation.

// COMMAND ----------

// MAGIC %python
// MAGIC import sklearn.neighbors
// MAGIC 
// MAGIC K = 1
// MAGIC knn = sklearn.neighbors.KNeighborsClassifier(n_neighbors=K)
// MAGIC knn.fit(points, label)

// COMMAND ----------

// MAGIC %md
// MAGIC Now, create a <a href="https://en.wikipedia.org/wiki/Voronoi_diagram" target="_blank">Voronoi diagram</a>, which will display the k-NN decision boundaries:
// MAGIC 
// MAGIC *Note: We merely use the Voronoi diagram for displaying our decision boundaries. Voronoi doesn't use our `knn` object, it rather makes its own nearest neighbor classification implicitly. We will use the predictions of `knn` in the next cells.*

// COMMAND ----------

// MAGIC %python
// MAGIC from scipy.spatial import Voronoi, voronoi_plot_2d
// MAGIC 
// MAGIC plt.gcf().clear()
// MAGIC 
// MAGIC vor = Voronoi(points)
// MAGIC voronoi_plot_2d(vor, show_points=False, line_width=0.5, line_alpha=0.5, show_vertices=False)
// MAGIC plt.scatter(points[:,0], points[:,1], c=[colors[i] for i in label], label=label, s=10, edgecolors='none')
// MAGIC plt.xlabel("x")
// MAGIC plt.ylabel("y")
// MAGIC 
// MAGIC display(plt.show())

// COMMAND ----------

// MAGIC %md
// MAGIC Let's predict!
// MAGIC 
// MAGIC In the next cell we are using the trained `knn` object to predict the labels to `points_to_predict`. The predicted values will show up as stars in the plot, having the predicted colors, respectively.

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC plt.gcf().clear()
// MAGIC 
// MAGIC # We are recreating the Voronoi diagram
// MAGIC vor = Voronoi(points)
// MAGIC voronoi_plot_2d(vor, show_points=False, line_width=0.5, line_alpha=0.5, show_vertices=False)
// MAGIC plt.scatter(points[:,0], points[:,1], c=[colors[i] for i in label], label=label, s=10, edgecolors='none')
// MAGIC plt.xlabel("x")
// MAGIC plt.ylabel("y")
// MAGIC 
// MAGIC # Come up with a few point for prediction
// MAGIC points_to_predict = np.array(
// MAGIC   [[10, 10],
// MAGIC    [20, 130],
// MAGIC    [100, 60],
// MAGIC    [80, 120],
// MAGIC    [120, 10],
// MAGIC    [140, 40],
// MAGIC    [65, 80],
// MAGIC    [30, 60],
// MAGIC    [70, 20]
// MAGIC   ])
// MAGIC 
// MAGIC # Run these points through k-NN 
// MAGIC predicted_labels = knn.predict(points_to_predict)
// MAGIC 
// MAGIC # Add them to the chart
// MAGIC plt.scatter(points_to_predict[:,0], points_to_predict[:,1], c=[colors[i] for i in predicted_labels], label=label, s=250, alpha=0.7, marker="*")
// MAGIC 
// MAGIC display(plt.show())

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>