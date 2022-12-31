// Databricks notebook source
// MAGIC 
// MAGIC %scala
// MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
// MAGIC val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
// MAGIC val username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")
// MAGIC val userhome = s"dbfs:/user/$username"
// MAGIC 
// MAGIC spark.conf.set("com.databricks.training.username", username)
// MAGIC spark.conf.set("com.databricks.training.userhome", userhome)
// MAGIC 
// MAGIC display(Seq())

// COMMAND ----------

def pathExists(path:String):Boolean = {
  try {
    dbutils.fs.ls(path)
    return true
  } catch{
    case e: Exception => return false
  } 
}

def cleanWorkspace(path:String):Unit = {
  if (pathExists(path)) {
    val result = dbutils.fs.rm(path, true)
    if (result == false) {
      throw new Exception("Unable to delete path: " + path)
    }
  }
}

val username = spark.conf.get("com.databricks.training.username")
val userhome = spark.conf.get("com.databricks.training.userhome")

cleanWorkspace(userhome + "/delta")

displayHTML("""
<div>Environment clean!</div>
""")

// COMMAND ----------

for (module_name <- Array("Create", "Append", "Upsert", "Streaming", "Optimization", "Architecture", "Time Travel", "Capstone")) {
  val databaseName = (username+"_"+module_name.toLowerCase()).replaceAll("[^a-zA-Z0-9]", "_") + "_dbs"
  println("Dropping " + databaseName)
  spark.sql("DROP DATABASE IF EXISTS %s CASCADE".format(databaseName))
}
