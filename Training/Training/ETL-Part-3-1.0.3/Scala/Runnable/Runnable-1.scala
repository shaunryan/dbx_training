// Databricks notebook source

dbutils.widgets.text("widget1", "defaultValue1")
dbutils.widgets.text("widget2", "defaultValue2")

// COMMAND ----------

val myVariable = 12

// COMMAND ----------

dbutils.notebook.exit("Notebook successfully executed.")
