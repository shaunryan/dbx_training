# Databricks notebook source

dbutils.widgets.text("date", "11-27-2013")
dbutils.widgets.text("dest_path", "tmp/raw_logs.parquet")

# COMMAND ----------

date = dbutils.widgets.get("date").strip()
dest_path = dbutils.widgets.get("dest_path").strip()

# COMMAND ----------

import json

def createTable(date, dest_path):
  '''
  Takes date and destination path, writes a parquet table to the destination path for that date's data
  '''
  path = "/mnt/training/UbiqLog4UCI/13_F/log_{}.txt".format(date)
  print("Importing file {}".format(path))
  
  df = (spark.read.json(path)
    .select("WiFi.*")
  )
  
  df.write.mode("OVERWRITE").parquet(dest_path)

  dbutils.notebook.exit(json.dumps({
    "status": "OK",
    "date": date,
    "path": dest_path
  }))

# COMMAND ----------

try:
  createTable(date, dest_path)
except: 
  print("File does not exist, check the date")
  
  dbutils.notebook.exit(json.dumps({
    "status": "FAILED",
    "date": date,
    "path": path
  }))
