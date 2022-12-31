# Databricks notebook source
# MAGIC 
# MAGIC %scala
# MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
# MAGIC val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC val username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")
# MAGIC val userhome = s"dbfs:/user/$username"
# MAGIC 
# MAGIC spark.conf.set("com.databricks.training.username", username)
# MAGIC spark.conf.set("com.databricks.training.userhome", userhome)
# MAGIC 
# MAGIC display(Seq())

# COMMAND ----------

import re

def pathExists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except:
    return False

def cleanWorkspace(path): 
  if pathExists(path):
    result = dbutils.fs.rm(path, True)
    if result == False:
      raise Exception("Unable to delete path: " + path)

username = spark.conf.get("com.databricks.training.username")
userhome = spark.conf.get("com.databricks.training.userhome")
      
cleanWorkspace(userhome + "/delta")

displayHTML("""
<div>Cleaned Files!</div>
""")

# COMMAND ----------

for module_name in ["Create", "Append", "Upsert", "Streaming", "Optimization", "Architecture", "Time Travel", "Capstone"]:
  databaseName = re.sub("[^a-zA-Z0-9]", "_", username+"_"+module_name.lower()) + "_dbp"
  print("Dropping " + databaseName)
  spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(databaseName))
