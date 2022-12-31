# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Production Issues
# MAGIC 
# MAGIC Deploying machine learning models is a complex process.  While some devops best practices from traditional software development apply, there are a number of additional concerns.  This lesson explores the various production issues encountered in deploying and monitoring machine learning models.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Introduce the primary concerns of production environments: reliability, scalability, and maintainability
# MAGIC  - Explore how model deployment differs from conventional software development deployment
# MAGIC  - Compare and contrast deployment architectures
# MAGIC  - Explore Continuous Integration and Continuous Deployment (CI/CD) for machine learning
# MAGIC  - Examine architecture options

# COMMAND ----------

# MAGIC %md
# MAGIC ###![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) From Data Science to Data Engineering
# MAGIC 
# MAGIC Data Science != Data Engineering<br><br>
# MAGIC 
# MAGIC * Data science and data engineering are two related but distinct practices
# MAGIC * Data scientists generally concern themselves with deriving business insights from data. They look to turn business problems into data problems, model those data problems, and optimize model performance.  
# MAGIC * Data engineers are generally concerned with a host of production issues:  
# MAGIC   - **Reliability:** The system should work correctly, even when faced with hardware and software failures or human error.
# MAGIC   - **Scalability:** The system should be able to deal with a growing and shrinking volume of data.
# MAGIC   - **Maintainability:** The system should be able to work with current demands and future feature updates.
# MAGIC 
# MAGIC <a href="https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/" target="_blank">Source</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/ASUMDM.png" style="height: 450px; margin: 20px"/></div>
# MAGIC 
# MAGIC We need the left side of this equation to be "closed loop" (that is, fully automated)
# MAGIC 
# MAGIC <a href="ftp://ftp.software.ibm.com/software/data/sw-library/services/ASUM.pdf" target="_blank">Source</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/Unified-ML-Deployment.png" style="height: 450px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reliability: Working Correctly<br><br>
# MAGIC 
# MAGIC  - **Fault tolerance** 
# MAGIC    - Hardware failures
# MAGIC    - Software failures
# MAGIC    - Human errors
# MAGIC  - **Robustness** 
# MAGIC    - Ad hoc queries
# MAGIC  - **Security**

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scalability: Growing<br><br>
# MAGIC 
# MAGIC  - **Throughput** 
# MAGIC    - Choose the best *load parameters* (e.g. GET or POST requests per second)
# MAGIC    - Question: what's the difference between 100k requests/sec of 1 KB each vs 3 requests/min of 2 GB each?
# MAGIC  - **Demand vs resources**
# MAGIC    - Vertical vs horizontal scalability
# MAGIC    - Linear scalability
# MAGIC    - Big O notation, % of task that's parallelizable 
# MAGIC  - **Latency and response time**
# MAGIC    - What is the speed at which applications respond to new requests?
# MAGIC    - Mean vs percentiles
# MAGIC    - Service Level Agreements (SLA's)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Maintainability: Running and Extending
# MAGIC 
# MAGIC The majority of the cost of software development is in ongoing maintenance.<br><br>
# MAGIC 
# MAGIC  - Operability (ease of operation)
# MAGIC    - Maintenance
# MAGIC    - Monitoring
# MAGIC    - Upgrading
# MAGIC    - Debuggability
# MAGIC  - Generalization and extensibility
# MAGIC    - Reuse of existing codebase assets (e.g. code, test suites, architectures) within a software application
# MAGIC    - The ability to extend a system to new feature demands and the level of effort required to implement an extension
# MAGIC    - Quantified ability to extend changes while minimizing impact to existing system functions
# MAGIC  - Automation
# MAGIC    - To what extent is an application a "closed loop" not requiring human oversight and intervention?
# MAGIC    
# MAGIC Helpful design pattern: decoupled storage and compute

# COMMAND ----------

# MAGIC %md
# MAGIC ###![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) DevOps vs "ModelOps"
# MAGIC 
# MAGIC DevOps = software development + IT operations<br><br>
# MAGIC 
# MAGIC * Manages the development life cycle
# MAGIC * Delivers features, patches, and updates
# MAGIC * Quality assurance
# MAGIC * Reduces friction between development, quality assurance, and production
# MAGIC * Strives to be agile (not waterfall)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC "ModelOps" = data modeling + deployment operations
# MAGIC 
# MAGIC The problem...<br><br>
# MAGIC 
# MAGIC * Data scientists use a zoo of different frameworks and languages.
# MAGIC * Scripting languages like Python and R are problematic in production.
# MAGIC * Data scientists _love_ libraries.  Many are not production ready.
# MAGIC * A chasm between academic and production solutions (scalability, reliability)
# MAGIC * Slow to update models
# MAGIC * Different code paths
# MAGIC 
# MAGIC Production means...<br><br>
# MAGIC 
# MAGIC * Deployment mostly in the Java ecosystem
# MAGIC   - Including Scala, which runs on the JVM
# MAGIC   - Also C/C++ and legacy environments 
# MAGIC * Using containers
# MAGIC 
# MAGIC **Promoting some basic design patterns in the data science workflow avoids many downstream production issues.**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Goals of "ModelOps"...<br><br>
# MAGIC 
# MAGIC * Bring machine learning into production
# MAGIC   - Model serialization
# MAGIC   - Refactor data science solutions
# MAGIC   - Containerizing 
# MAGIC * Add model performance to testing and monitoring
# MAGIC * Reduce time between model development and deployment

# COMMAND ----------

# MAGIC %md
# MAGIC Other considerations...<br><br>
# MAGIC 
# MAGIC * Training vs prediction time
# MAGIC   - Some models have high training but low prediction time, and vice versa.
# MAGIC * IO vs CPU bound problems
# MAGIC * Live training algorithms

# COMMAND ----------

# MAGIC %md
# MAGIC Other Technologies and Frameworks...<br><br>
# MAGIC 
# MAGIC * <a href="https://www.kubeflow.org/#overview" target="_blank">Kubeflow</a>: Data engineering focused way to manage the ML process
# MAGIC * <a href="https://code.fb.com/core-data/introducing-fblearner-flow-facebook-s-ai-backbone/" target="_blank">FBLearner</a>: Another way to manage the ML process
# MAGIC * <a href="https://github.com/databricks/spark-sklearn" target="_blank">Spark-sklearn</a>: Model tuning at scale with Spark
# MAGIC * <a href="http://mleap-docs.combust.ml/" target="_blank">MLeap</a>: Serialization format and execution engine for machine learning pipelines (`Spark`, `sklearn`, `TensorFlow`)
# MAGIC   - <a href="https://docs.databricks.com/spark/latest/mllib/mleap-model-export.html" target="_blank">See the Databricks docs for a runthrough</a>
# MAGIC * <a href="https://onnx.ai/supported-tools" target="_blank">Onnx</a>: A community project created by Facebook and Microsoft for greater interoperability in the AI tools community
# MAGIC 
# MAGIC Managing Deployments...<br><br>
# MAGIC 
# MAGIC  - SageMaker
# MAGIC  - Azure ML

# COMMAND ----------

# MAGIC %md
# MAGIC Model registry requirements...<br><br>
# MAGIC 
# MAGIC - Stores models as first class citizens
# MAGIC   - trained
# MAGIC   - deployed
# MAGIC - Metadata 
# MAGIC   - origin story
# MAGIC   - metrics
# MAGIC   - monitoring
# MAGIC   - telemetry
# MAGIC - Model version control
# MAGIC - Rollbacks on failure

# COMMAND ----------

# MAGIC %md
# MAGIC ###![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Three Architectures
# MAGIC 
# MAGIC Batch processing (Table Query)<br><br>
# MAGIC 
# MAGIC   - Example: churn prediction where high predicted churn generates targeted marketing
# MAGIC   - Most deployments are batch (approx 80%)
# MAGIC   - Save predictions to database
# MAGIC   - Query database in order to serve the predictions

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/deployment-options.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC Continuous/Real-Time (Streaming)<br><br>
# MAGIC 
# MAGIC   - Typically asynchronous
# MAGIC   - Example: Live predictions on a stream of data (e.g. web activity logs)
# MAGIC   - Predict using a pre-trained model
# MAGIC   - Latency in seconds
# MAGIC   - Often using Spark

# COMMAND ----------

# MAGIC %md
# MAGIC On demand (REST, Embedded)<br><br>
# MAGIC 
# MAGIC   - Typically synchronous
# MAGIC   - Millisecond latency
# MAGIC   - Normally served by REST or RMI
# MAGIC   - Example: live fraud detection on credit card transaction requests
# MAGIC   - Serialized or containerized model saved to a blob store or HDFS
# MAGIC   - Served using another engine (e.g. SageMaker or Azure ML) 
# MAGIC   - Latency in milliseconds
# MAGIC   - Could involve live model training

# COMMAND ----------

# MAGIC %md
# MAGIC Phases of Deployment<br><br>
# MAGIC 
# MAGIC - dev/test/staging/production
# MAGIC - Model versioning
# MAGIC - When you should retrain, what you should retrain on (e.g. a trailing window, all data)
# MAGIC - Warm starts

# COMMAND ----------

# MAGIC %md
# MAGIC A/B Testing<br><br>
# MAGIC 
# MAGIC - Technologies include Clipper, Optimizely, and Split IO   
# MAGIC - Can also be done with batch inference in a custom way

# COMMAND ----------

# MAGIC %md
# MAGIC ###![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Continuous Integration and Continuous Deployment
# MAGIC 
# MAGIC Continuous...<br><br>
# MAGIC 
# MAGIC * Integration: developers push changes often to build and run automated tests
# MAGIC * Delivery: automated, easy ways of deploying an application
# MAGIC * Deployment: one step further than continuous delivery, automated deployment has no human intervention in deployment

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/Model-Staleness.png" style="height: 450px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC Solutions...<br><br>
# MAGIC 
# MAGIC  * Bamboo (confluence, might be replaced)
# MAGIC  * TeamCity
# MAGIC  * Jenkins
# MAGIC  * Git Lab
# MAGIC  * Travis CI
# MAGIC  * Airflow
# MAGIC  * Azure CI/CD pipelines
# MAGIC  * Amazon CodePipeline
# MAGIC  
# MAGIC <a href="https://databricks.com/blog/2017/10/30/continuous-integration-continuous-delivery-databricks.html" target="_blank">See blog for incorporation of Databricks in a CI/CD Pipeline</a> 

# COMMAND ----------

# MAGIC %md
# MAGIC ###![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Common Architectures

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/ml-architecture-1.png" style="height: 450px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/ml-architecture-2.png" style="height: 450px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Next Lesson<br>
# MAGIC 
# MAGIC ### Start the next lesson, [Batch Deployment]($./03-Batch-Deployment )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC **Q:** Are there additional tools I can use for a high level view of production machine learning issues?  
# MAGIC **A:** See <a href="https://pages.databricks.com/ProductionizingMLWebinar_ty-wb-databricks-mlflow.html" target="_blank">the Databricks webinar _Productionalizing Machine Learning_</a>.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>