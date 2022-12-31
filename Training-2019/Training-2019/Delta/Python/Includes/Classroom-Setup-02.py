# Databricks notebook source
# MAGIC 
# MAGIC %python
# MAGIC module_name = "Create"
# MAGIC spark.conf.set("com.databricks.training.module_name", module_name)
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

# COMMAND ----------

# MAGIC %run ./Classroom-Setup

# COMMAND ----------

basePath = userhome + "/delta-il02p"
cleanWorkspace(basePath)

displayHTML("""
<div>Declared module-specific resources:</div>
<li>Declared <b style="color:green">basePath:String</b> = <b style="color:blue">%s</b></li>
""" % basePath)

# COMMAND ----------

cleanWorkspace(basePath)
displayHTML("Removed temp files from " + basePath)
