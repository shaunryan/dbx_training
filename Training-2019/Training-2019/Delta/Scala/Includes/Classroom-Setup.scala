// Databricks notebook source
// MAGIC 
// MAGIC %python
// MAGIC course_name = "Databricks Delta"
// MAGIC spark.conf.set("com.databricks.training.course_name", course_name)

// COMMAND ----------

// MAGIC %run "./Dataset-Mounts"

// COMMAND ----------

// MAGIC %run "./Create-User-DB"

// COMMAND ----------

// MAGIC %run "./Test-Library"

// COMMAND ----------

def untilStreamIsReady(name:String):Unit = {
  val queries = spark.streams.active.filter(_.name == name)

  if (queries.length == 0) {
    println("The stream is not active.")
  } else {
    while (queries(0).isActive && queries(0).recentProgress.length == 0) {
      // wait until there is any type of progress
    }

    if (queries(0).isActive) {
      queries(0).awaitTermination(5*1000)
      println("The stream is active and ready.")
    } else {
      println("The stream is not active.")
    }
  }
}

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

def deleteTables(database:String):Unit = {
  spark.sql("DROP DATABASE IF EXISTS %s CASCADE".format(database))
}

displayHTML("""
<div>Declared course-specific utility methods:</div>
<li>Declared <b style="color:green">untilStreamIsReady(<i>name:String</i>)</b> to control workflow</li>
""")
