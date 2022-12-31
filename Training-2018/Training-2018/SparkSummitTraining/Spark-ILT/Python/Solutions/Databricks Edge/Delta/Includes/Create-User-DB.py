# Databricks notebook source
# MAGIC %scala
# MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
# MAGIC val username = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC val databaseName   = username.replaceAll("[^a-zA-Z0-9]", "_") + "_db"
# MAGIC 
# MAGIC spark.sql(s"CREATE DATABASE IF NOT EXISTS `$databaseName`")
# MAGIC spark.sql(s"USE `$databaseName`")
# MAGIC 
# MAGIC spark.conf.set("com.databricks.training.spark.databaseName", databaseName)
# MAGIC 
# MAGIC displayHTML("Created User Database...") // suppress output

# COMMAND ----------

# MAGIC %python
# MAGIC databaseName = spark.conf.get("com.databricks.training.spark.databaseName")