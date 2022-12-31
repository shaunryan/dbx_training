# Databricks notebook source
# MAGIC 
# MAGIC %python
# MAGIC course_name = "Databricks Delta"
# MAGIC spark.conf.set("com.databricks.training.course_name", course_name)

# COMMAND ----------

# MAGIC %run "./Dataset-Mounts"

# COMMAND ----------

# MAGIC %run "./Create-User-DB"

# COMMAND ----------

# MAGIC %run "./Test-Library"

# COMMAND ----------

def untilStreamIsReady(name):
  queries = list(filter(lambda query: query.name == name, spark.streams.active))

  if len(queries) == 0:
    print("The stream is not active.")

  else:
    while (queries[0].isActive and len(queries[0].recentProgress) == 0):
      pass # wait until there is any type of progress

    if queries[0].isActive:
      queries[0].awaitTermination(5)
      print("The stream is active and ready.")
    else:
      print("The stream is not active.")


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
    

def deleteTables(database):
  spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(database))

displayHTML("""
<div>Declared course-specific utility methods:</div>
<li>Declared <b style="color:green">untilStreamIsReady(<i>name:String</i>)</b> to control workflow</li>
""")
