// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Apache&reg; Spark&trade; for Machine Learning and Data Science
// MAGIC ## Databricks Spark 301 (3 Days)  
// MAGIC See **<a href="https://databricks.com/training/courses/apache-spark-for-machine-learning-and-data-science" target="_blank">https&#58;//databricks.com/training/courses/apache-spark-for-machine-learning-and-data-science</a>**

// COMMAND ----------

// MAGIC %md
// MAGIC ## Day #1 AM
// MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
// MAGIC |:----:|-------|-------------|
// MAGIC | 30m  | **Introductions**                                                                ||
// MAGIC | 20m  | **Setup**                                                                        | *Registration, Courseware & Q&As* |
// MAGIC | 10m  | **Break**                                                                        ||
// MAGIC ||||
// MAGIC | 50m  | **[Apache Spark Overview]($./Apache Spark Overview)**                            | *About Databricks, Spark & Spark Architecture* |
// MAGIC | 10m  | **Break**                                                                        ||
// MAGIC ||||
// MAGIC | 30m  | **[Reading Data - CSV]($./Reading & Writing Data/Reading Data 1 - CSV)**         | *Spark Entry Point, Reading Data, Inferring Schemas, API Docs* |
// MAGIC | 10m  | **[Reading Data - Summary]($./Reading & Writing Data/Reading Data 7 - Summary)** | *Review and contrast the differences of various readers* | 
// MAGIC | 10m  | **[Lab: Reading Data]($./Reading & Writing Data/Reading Data 8 - Lab)**           | *Putting to practice what we just learned*<br/>*(completed collaboratively)* |
// MAGIC | 10m  | **[Writing Data]($./Reading & Writing Data/Writing Data)**                       | *Quick intro to DataFrameWriters* |

// COMMAND ----------

// MAGIC %md
// MAGIC ## Day #1 PM
// MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
// MAGIC |:----:|-------|-------------|
// MAGIC | 50m  | **[Intro To DataFrames Part-1]($./Intro To DataFrames/Intro To DF Part 1)**      | *API Docs, DataFrames, cache(), show(), display(), limit()*<br/>*count(), select(), drop(), distinct(), SQL, Temp Views* |
// MAGIC | 10m  | **[Lab: Distinct Articles]($./Intro To DataFrames/Intro To DF Part 1 Lab)**      | *Putting to practice what we just learned*<br/>*(completed collaboratively)* |
// MAGIC | 10m  | **Break** || 
// MAGIC | 50m  | **[Partitioning]($./Other Topics/Partitioning)**                                 | *Partitions vs. Slots, repartition(n), coalesce(n), spark.sql.shuffle.partitions* |
// MAGIC | 10m  | **Break** || 
// MAGIC | 50m  | **[Intro To DataFrames Part-2]($./Intro To DataFrames/Intro To DF Part 2)**      | *orderBy(), Column, filter(), firs(), Row, collect(), take(n), Dataframe vs. DataSet* |
// MAGIC | 10m  | **[Lab: Washingtons and Adams]($./Intro To DataFrames/Intro To DF Part 2 Lab)**  | *Counting & summing Washingtons*<br/>*(completed collaboratively)* | 
// MAGIC | 10m  | **Break** || 
// MAGIC | 10m  | **[Introduction to Structured Streaming]($./Structured Streaming/Structured Streaming 1 - Intro)**       | *Micro-Batches, Input & Results Table, Ouput Modes & Sinks* |

// COMMAND ----------

// MAGIC %md
// MAGIC ## Day #2 AM
// MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
// MAGIC |:----:|-------|-------------|
// MAGIC | 20m  | **Review**                                                                       | *What did we discover yesterday?* |     
// MAGIC | 30m  | **[Structured Streaming Examples]($./Structured Streaming/Structured Streaming 2 - TCPIP)**    | *DataStreamReader, Limitations, Windowing, Watermarking, Checkpointing, Fault Tolerance* |
// MAGIC | 10m  | **[Lab: Analyzing Streamings]($./Structured Streaming/Structured Streaming 4 - Lab)**        | *Analyise our stream, aggregating by IP Addresses*<br/>*(completed collaboratively)* |
// MAGIC ||||
// MAGIC | 10m  | **Break** || 
// MAGIC | 50m  | **[Intro To DataFrames Part-3]($./Intro To DataFrames/Intro To DF Part 3)**   | *withColumnRenamed(), withColumn(), unix_timestamp() & cast()*<br/>*year(), month(), dayofyear(), RelationalGroupedDataset, sum(), count(), avg(), min(), max()* |
// MAGIC ||||
// MAGIC | 10m  | **Break** || 
// MAGIC | 40m  | **[Lab: De-Duping Data]($./Intro To DataFrames/Intro To DF Part 3 Lab)**          | *Real world problem solving - removing duplicate records* |

// COMMAND ----------

// MAGIC %md
// MAGIC ## Day #2 PM
// MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
// MAGIC |:----:|-------|-------------|
// MAGIC | 20m  | **Intro to ML** | *Supervised vs Unsupervised ML, Evaluation metrics, etc.* |
// MAGIC | 50m  | **[ML 01 - SparkML Demo]($./Machine Learning/ML 01 - SparkML Demo)** | *Introduction to Spark ML (Decision Tree overview, Data Preparation, Transformers and Evaluators: Tokenization, CountVectorizer, Binarizer)* |
// MAGIC | 10m  | **Break** | |
// MAGIC | 10m  | **[ML 02 - SparkML Streaming]($./Machine Learning/ML 02 - SparkML Streaming)** | *Apply SparkML Pipeline to Streaming Data*|
// MAGIC | 40m  | **[ML 03 - SparkML Lab]($./Machine Learning/ML 03 - SparkML Lab)** | *Self-guided lab*|
// MAGIC | 10m  | **Break** | |
// MAGIC | 70m  | **[ML 04 - Random Forests]($./Machine Learning/ML 04 - Random Forests)** | *Intro to Bootstrapping and bagging; Hyperparameter search with cross-validation* |

// COMMAND ----------

// MAGIC %md
// MAGIC ## Day #3 AM
// MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
// MAGIC |:----:|-------|-------------|
// MAGIC | 20m  | **Review** | *What did we discover yesterday?* |     
// MAGIC | 10m  | **[ML 05 - Data Cleansing]($./Machine Learning/ML 05 - Data Cleansing)** | *Preparing a dataset for data scientists* |
// MAGIC | 35m  | **[ML 06 - Data Cleansing Lab]($./Machine Learning/ML 06 - Data Cleansing Lab)** | *Self-guided Lab on Airbnb Data* |
// MAGIC | 10m  | **Break** | |
// MAGIC | 45m  | **[ML 07 - Linear Regression Lab]($./Machine Learning/ML 07 - Linear Regression Lab)** | *Self-guided Lab on building a model with Airbnb data* |
// MAGIC | 20m  | **[ML 08 - Gradient Boosted Trees]($./Machine Learning/ML 08 - GBDT)** | *Improving upon linear regression model with Gradient-boosted trees (XGBoost and LightGBM), log-normal scale* |
// MAGIC | 10m  | **Break** | |
// MAGIC | 35m | **[ML 09 - spark-sklearn]($./Machine Learning/ML 09 - spark-sklearn)** | *Building multiple scikit-learn models in parallel on Spark* |

// COMMAND ----------

// MAGIC %md
// MAGIC ## Day #3 PM
// MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
// MAGIC |:----:|-------|-------------|
// MAGIC | 35m  | **[ML 10 - KMeans]($./Machine Learning/ML 10 - KMeans)** | *Unsupervised ML: Clustering* |
// MAGIC | 10m  | **Break** | |
// MAGIC | 60m  | **[ML 11 - ALS Prediction]($./Machine Learning/ML 11 - ALS Prediction)** | *Self guided lab on making personalized movie recommendations* |
// MAGIC | 10m  | **Break** | | 
// MAGIC | 20m  | **[ML 11 - ALS Prediction]($./Machine Learning/ML 11 - ALS Prediction)** | *Solutions* |
// MAGIC | 30m  | **ML Electives** | *Optional Electives from list below* |
// MAGIC | 30m  | **Wrapping up** | *Course summary, Further Reading, Q&A* |

// COMMAND ----------

// MAGIC %md
// MAGIC ## ML Electives
// MAGIC | Time | Type | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
// MAGIC |:----:|:----:|-------|-------------|
// MAGIC | | ... | **[MLE 01 - GraphFrames]($./Machine Learning/ML Electives/MLE 01 - GraphFrames)** | *Graph Analytics of College Football games* |
// MAGIC | | ... | **[MLE 02 - Topic Modeling with LDA]($./Machine Learning/ML Electives/MLE 02 - Topic Modeling with LDA)** | *Latent Dirichlet Allocation Topic Modeling of UseNet messages* |

// COMMAND ----------

// MAGIC %md
// MAGIC The times indicated here are approximated only - actual times will vary by class size, class participation, and other unforeseen factors.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>