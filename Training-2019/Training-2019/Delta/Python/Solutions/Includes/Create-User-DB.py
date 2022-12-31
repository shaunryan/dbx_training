# Databricks notebook source
# MAGIC 
# MAGIC %scala
# MAGIC val dbNamePrefix = {
# MAGIC   val tags = com.databricks.logging.AttributionContext.current.tags
# MAGIC   val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC   val username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")
# MAGIC   
# MAGIC   val module_name = spark.conf.get("com.databricks.training.module_name").toLowerCase()
# MAGIC 
# MAGIC   val dbNamePrefix = (username+"_"+module_name).replaceAll("[^a-zA-Z0-9]", "_") + "_db"
# MAGIC   spark.conf.set("com.databricks.training.spark.dbNamePrefix", dbNamePrefix)
# MAGIC   dbNamePrefix
# MAGIC }
# MAGIC 
# MAGIC displayHTML(s"Created user-specific database")

# COMMAND ----------

dbNamePrefix = spark.conf.get("com.databricks.training.spark.dbNamePrefix")
databaseName = dbNamePrefix + "ilp"
spark.conf.set("com.databricks.training.spark.databaseName", databaseName)

spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(databaseName))
spark.sql("USE {}".format(databaseName))

displayHTML("""Using the database <b style="color:green">{}</b>.""".format(databaseName))
