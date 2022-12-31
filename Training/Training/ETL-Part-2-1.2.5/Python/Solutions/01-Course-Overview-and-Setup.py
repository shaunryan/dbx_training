# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Course Overview and Setup
# MAGIC ## ETL Part 2: Transformations and Loads
# MAGIC 
# MAGIC In this course, Data Engineers apply data transformation and extraction best practices such as user-defined functions, efficient table joins, and parallel database writes.  
# MAGIC By the end of this course, you will transform complex data with custom functions, load it into a target database, and navigate Databricks and Spark documents to source solutions.
# MAGIC 
# MAGIC ** The course includes the following lessons:**
# MAGIC 1. Course Overview and Setup
# MAGIC 1. Common Transformations
# MAGIC 1. User Defined Functions
# MAGIC 1. Advanced UDFs
# MAGIC 1. Joins and Lookup Tables
# MAGIC 1. Database Writes
# MAGIC 1. Table Management
# MAGIC 1. Capstone Project: Custom Transformations, Aggregating and Loading

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/xksj642zx1?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/xksj642zx1?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Getting Started on Databricks
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Differentiate stages in a common ETL workflow
# MAGIC * Log into Databricks
# MAGIC * Create a notebook inside your home folder in Databricks
# MAGIC * Create or attach to a Spark cluster
# MAGIC * Import the course files into your home folder
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers
# MAGIC * Additional Audiences: Data Scientists and Data Pipeline Engineers
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: Chrome or Firefox
# MAGIC * Concept (optional): <a href="https://academy.databricks.com/collections/frontpage/products/etl-part-1-data-extraction" target="_blank">ETL Part 1 course from Databricks Academy</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Raw, Query, and Summary Tables
# MAGIC 
# MAGIC A number of different terms describe the movement of data through an ETL pipeline. For course purposes, data begins in the pipeline with **raw tables.** This refers to data that arrives in the pipeline, conforms to a schema, and does not include any sort of parsing or aggregation.
# MAGIC 
# MAGIC Raw tables are then parsed into query-ready tables, known as **query tables.**  Query tables might populate a relational data model for ad hoc (OLAP) or online (OLTP) queries, but generally do not include any sort of aggregation such as daily averages.  Put another way, query tables are cleaned and filtered data.
# MAGIC 
# MAGIC Finally, **summary tables** are business level aggregates often used for reporting and dashboarding. This includes aggregations such as daily active website visitors.
# MAGIC 
# MAGIC It is a good idea to preserve raw tables because it lets you adapt to future needs not considered in the original data model or correct parsing errors. This pipeline ensures data engineers always have access to the data they need, while reducing the barriers to common data access patterns. Data becomes more valuable and easier to access in each subsequent stage of the pipeline.  
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-2/gold-silver-bronze.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC To get up and running, create a notebook and Spark cluster. If you already have a cluster running and are familiar with Databricks, feel free to skip ahead to the next lesson.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/46ztztgeuq?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/46ztztgeuq?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 1
# MAGIC Databricks notebooks are backed by clusters, or networked computers, that process data. Create a Spark cluster (*if you already have a running cluster, skip to **Step 3** *):
# MAGIC 1. Select the **Clusters** icon in the sidebar.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-cluster-4.png" style="height: 200px; margin: 20px"/></div>
# MAGIC 2. Click the **Create Cluster** button.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-cluster-5.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC 3. Name your cluster. Use your name or initials to easily differentiate your cluster from your coworkers.
# MAGIC 4. Select the cluster type; recommendation is the latest Databricks runtime (**3.3**, **3.4**, etc.) and Scala **2.11**.
# MAGIC 5. Specify the cluster configuration.
# MAGIC   * For clusters created on a **Community Edition** shard, the default values are sufficient for the remaining fields.
# MAGIC   * For all other shards, please refer to your company's policy on private clusters.</br></br>
# MAGIC 6. Click the **Create Cluster** button.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-cluster-2.png" style="height: 300px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC 
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Check with your local system administrator to see if there is a recommended default cluster at your company to use for the rest of the class. This could save you  money!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2
# MAGIC 
# MAGIC Create a new notebook in your home folder.
# MAGIC 1. Select the **Home** icon in the sidebar.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/home.png" style="height: 200px; margin: 20px"/></div>
# MAGIC 2. Right-click your home folder.
# MAGIC 3. Select **Create**.
# MAGIC 4. Select **Notebook**.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-notebook-1.png" style="height: 150px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC 5. Name your notebook `My Notebook`.<br/>
# MAGIC 6. Set the language to **Python**.<br/>
# MAGIC 7. Select the cluster to attach this Notebook.
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> If a cluster is not currently running, this option will not exist.
# MAGIC 8. Click **Create**.
# MAGIC <div>
# MAGIC   <div style="float:left"><img src="https://files.training.databricks.com/images/eLearning/create-notebook-2b.png" style="width:400px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC   <div style="float:left; margin-left: e3m; margin-right: 3em">or</div>
# MAGIC   <div style="float:left"><img src="https://files.training.databricks.com/images/eLearning/create-notebook-2.png" style="width:400px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC   <div style="clear:both"></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 3
# MAGIC 
# MAGIC Once you have a notebook, use it to run code
# MAGIC 1. In the first cell of your notebook, type `1 + 1`
# MAGIC 2. Run the cell: Click the **Run** icon and then select **Run Cell**
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/run-notebook-1.png" style="width:600px; margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> **Ctrl-Enter** also runs a cell

# COMMAND ----------

1 + 1

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Attach and Run
# MAGIC 
# MAGIC If your notebook was not previously attached to a cluster you might receive the following prompt:
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/run-notebook-2.png" style="margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC 
# MAGIC If you click **Attach and Run**, first make sure you attach to the correct cluster.
# MAGIC 
# MAGIC If it is not the correct cluster, click **Cancel** instead see the next cell, **Attach & Detach**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Attach & Detach
# MAGIC 
# MAGIC If your notebook is detached you can attach it to another cluster:
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/attach-to-cluster.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/>
# MAGIC 
# MAGIC If your notebook is attached to a cluster you can:
# MAGIC * Detach your notebook from the cluster
# MAGIC * Restart the cluster
# MAGIC * Attach to another cluster
# MAGIC * Open the Spark UI
# MAGIC * View the Driver's log files
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/detach-from-cluster.png" style="margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC * Click the down arrow on a folder and select the **Create Notebook** option to create notebooks
# MAGIC * Click the down arrow on a folder and select the **Import** option to import notebooks
# MAGIC * Select the **Attached/Detached** option directly below the notebook title to attach to a Spark cluster
# MAGIC * Create clusters using the **Clusters** button on the left sidebar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC 
# MAGIC **Question:** How do you create a Notebook?  
# MAGIC **Answer:** Sign into Databricks, select the **Home** icon from the sidebar, right-click your home-folder, select **Create**, and then **Notebook**. In the **Create Notebook** dialog, specify the name of your notebook and the default programming language.
# MAGIC 
# MAGIC **Question:** How do you create a cluster?  
# MAGIC **Answer:** Select the **Clusters** icon on the sidebar, click the **Create Cluster** button, specify the specific settings for your cluster and then click **Create Cluster**.
# MAGIC 
# MAGIC **Question:** How do you attach a notebook to a cluster?  
# MAGIC **Answer:** If you run a command while detached, you may be prompted to connect to a cluster. To connect to a specific cluster, open the cluster menu by clicking the **Attached/Detached** menu item and then selecting your desired cluster.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Next Steps
# MAGIC 
# MAGIC This course is available in Python and Scala.  Start the next lesson, **Common Transformations**.
# MAGIC 1. Click the **Home** icon in the left sidebar
# MAGIC 2. Select your home folder
# MAGIC 3. Select the folder **ETL-Part-2**
# MAGIC 4. Open the notebook **02-Common-Transformations** in either the Python or Scala folder
# MAGIC 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/ETL-Part-2/Course-Import2.png" style="margin-bottom: 5px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; width: auto; height: auto; max-height: 383px"/>
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The Python and Scala content is identical except for the language used.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC **Q:** Are there additional docs I can reference to find my way around Databricks?  
# MAGIC **A:** See <a href="https://docs.databricks.com/user-guide/getting-started.html" target="_blank">Getting Started with Databricks</a>.
# MAGIC 
# MAGIC **Q:** Where can I learn more about the cluster configuration options?  
# MAGIC **A:** See <a href="https://docs.databricks.com/user-guide/clusters/index.html" target="_blank">Spark Clusters on Databricks</a>.
# MAGIC 
# MAGIC **Q:** Can I import formats other than .dbc files?  
# MAGIC **A:** Yes, see <a href="https://docs.databricks.com/user-guide/notebooks/index.html#importing-notebooks" target="_blank">Importing Notebooks</a>.
# MAGIC 
# MAGIC **Q:** Can I use browsers other than Chrome or Firefox?  
# MAGIC **A:** Databricks is tested for Chrome and Firefox.  It does work on Internet Explorer 11 and Safari however, it is possible some user-interface features may not work properly.
# MAGIC 
# MAGIC **Q:** Can I install the courseware notebooks into a non-Databricks distribution of Spark?  
# MAGIC **A:** No, the files that contain the courseware are in a Databricks specific format (DBC).
# MAGIC 
# MAGIC **Q:** Do I have to have a paid Databricks subscription to complete this course?  
# MAGIC **A:** No, you can sign up for a free <a href="https://databricks.com/try-databricks" target="_blank">Community Edition</a> account from Databricks.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>