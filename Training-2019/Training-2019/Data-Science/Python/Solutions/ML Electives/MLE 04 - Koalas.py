# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Koalas
# MAGIC 
# MAGIC The Koalas project aims at providing the pandas DataFrame API on top of Apache Spark. Thus, unifying the two ecosystems with a familiar API, and offering a seamless transition between small and large data.
# MAGIC 
# MAGIC [Koalas Docs](https://koalas.readthedocs.io/en/latest/index.html)
# MAGIC 
# MAGIC [Koalas Github](https://github.com/databricks/koalas)
# MAGIC 
# MAGIC **Required Libraries**:
# MAGIC * koalas==0.15.0

# COMMAND ----------

# MAGIC %md
# MAGIC We will be using the [UCI Machine Learning Repository 
# MAGIC Bank Marketing Data Set](https://archive.ics.uci.edu/ml/datasets/bank+marketing) in this demo.

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %sh wget https://archive.ics.uci.edu/ml/machine-learning-databases/00222/bank.zip -O /tmp/bank.zip

# COMMAND ----------

# MAGIC %sh unzip -o /tmp/bank.zip -d /tmp/bank

# COMMAND ----------

# MAGIC %python
# MAGIC file_path = userhome + "/bank-full.csv"
# MAGIC dbutils.fs.cp("file:/tmp/bank/bank-full.csv", file_path)

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.head(file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data as PySpark Dataframe

# COMMAND ----------

# MAGIC %python
# MAGIC df = (spark
# MAGIC       .read
# MAGIC       .option("inferSchema", "true")
# MAGIC       .option("header", "true")
# MAGIC       .option("delimiter", ";")
# MAGIC       .option("quote", '"')
# MAGIC       .csv(file_path))
# MAGIC 
# MAGIC display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data as pandas Dataframe

# COMMAND ----------

# MAGIC %python
# MAGIC import pandas as pd
# MAGIC import databricks.koalas as ks
# MAGIC 
# MAGIC csv_path = file_path.replace("dbfs:/", "/dbfs/")
# MAGIC 
# MAGIC # Read in using panas read_csv
# MAGIC pdf = pd.read_csv(csv_path, header=0, sep=";", quotechar='"')
# MAGIC 
# MAGIC display(pdf.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert to Koalas Dataframe

# COMMAND ----------

# MAGIC %python
# MAGIC # Create a Koalas DataFrame from pandas DataFrame
# MAGIC kdf = ks.from_pandas(pdf)
# MAGIC 
# MAGIC # Alternative for creating Koalas DataFrame from pandas DataFrame
# MAGIC # kdf = ks.DataFrame(pdf)
# MAGIC 
# MAGIC # Create a Koalas DataFrame from PySpark DataFrame
# MAGIC # kdf = ks.DataFrame(df)
# MAGIC 
# MAGIC kdf

# COMMAND ----------

# MAGIC %python
# MAGIC # Koalas Dataframe -> PySpark DataFrame
# MAGIC kdf.to_spark()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column Manipulation

# COMMAND ----------

# MAGIC %python
# MAGIC # Adding a column with Koalas
# MAGIC kdf["duration_new"] = kdf["duration"] + 100
# MAGIC 
# MAGIC kdf.head()

# COMMAND ----------

# MAGIC %python
# MAGIC # Adding a column with PySpark
# MAGIC from pyspark.sql.functions import col
# MAGIC 
# MAGIC df = df.withColumn("duration_new", col("duration") + 100)
# MAGIC display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Filtering

# COMMAND ----------

# MAGIC %python
# MAGIC # Filtering with Koalas
# MAGIC kdf_filtered = kdf[kdf.duration_new >= 300]
# MAGIC 
# MAGIC kdf_filtered.head()

# COMMAND ----------

# MAGIC %python
# MAGIC # Filtering with PySpark
# MAGIC df_filtered  = df.filter(col("duration_new") >= 300)
# MAGIC 
# MAGIC display(df_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Value Counts

# COMMAND ----------

# MAGIC %python
# MAGIC # GroupBy in pandas
# MAGIC pdf["job"].value_counts()

# COMMAND ----------

# MAGIC %python
# MAGIC # GroupBy in koalas
# MAGIC kdf["job"].value_counts()

# COMMAND ----------

# MAGIC %python
# MAGIC # To get same output with PySpark
# MAGIC display(df.groupby("job").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ###GroupBy

# COMMAND ----------

# MAGIC %python
# MAGIC # Get average age per education group in Koalas
# MAGIC kdf_grouped_1 = kdf.groupby("education").agg({"age": "mean"}).reset_index()
# MAGIC 
# MAGIC # Rename cols
# MAGIC kdf_grouped_1.columns = ["education", "avg_age"]
# MAGIC kdf_grouped_1

# COMMAND ----------

# MAGIC %python
# MAGIC # Get the maximum balance for each education group in Koalas
# MAGIC kdf_grouped_2 = kdf.groupby("education").agg({"balance": "max"}).reset_index()
# MAGIC kdf_grouped_2.columns = ["education", "max_balance"]
# MAGIC kdf_grouped_2

# COMMAND ----------

# MAGIC %python
# MAGIC # Get average age per education group in PySpark
# MAGIC df_grouped_1 = (df.groupby("education")
# MAGIC                 .agg({"age": "mean"})
# MAGIC                 .select("education", col("avg(age)").alias("avg_age")))
# MAGIC df_grouped_1
# MAGIC display(df_grouped_1)

# COMMAND ----------

# MAGIC %python
# MAGIC # Get the maximum balance for each education group in PySpark
# MAGIC df_grouped_2 = (df.groupby("education")
# MAGIC                 .agg({"balance": "max"})
# MAGIC                 .select("education", col("max(balance)").alias("max_balance")))
# MAGIC df_grouped_2
# MAGIC display(df_grouped_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joins

# COMMAND ----------

# MAGIC %python
# MAGIC # Joining the grouped DataFrames on education using Koalas
# MAGIC kdf_edu_joined = kdf_grouped_1.merge(kdf_grouped_2, on="education", how="inner")
# MAGIC kdf_edu_joined

# COMMAND ----------

# MAGIC %python
# MAGIC # Joining the grouped DataFrames on education using PySpark
# MAGIC df_edu_joined = df_grouped_1.join(df_grouped_2, on="education", how="inner")
# MAGIC display(df_edu_joined)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>