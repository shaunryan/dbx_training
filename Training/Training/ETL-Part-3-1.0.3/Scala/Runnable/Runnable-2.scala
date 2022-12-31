// Databricks notebook source

dbutils.widgets.text("date", "11-27-2013")
dbutils.widgets.text("dest_path", "tmp/raw_logs.parquet")

// COMMAND ----------

val date = dbutils.widgets.get("date").stripMargin
val dest_path = dbutils.widgets.get("dest_path").stripMargin

// COMMAND ----------

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper

// Create a json serializer
val jsonMapper = new ObjectMapper with ScalaObjectMapper
jsonMapper.registerModule(DefaultScalaModule)

def createTable(date: String, dest_path: String, jsonMapper: ObjectMapper): Unit = {
  val path = s"/mnt/training/UbiqLog4UCI/13_F/log_$date.txt"
  println(s"Importing file $path")
  val df = spark.read.json(path)
    .select("WiFi.*")
  
  df.write.mode("OVERWRITE").parquet(dest_path)
  
  dbutils.notebook.exit(jsonMapper.writeValueAsString(Map("status" -> "OK", "date" -> date, "path" -> dest_path)))
}

// COMMAND ----------

try {
  createTable(date, dest_path, jsonMapper)
} catch {
  case e: org.apache.spark.sql.AnalysisException => dbutils.notebook.exit(jsonMapper.writeValueAsString(Map("status" -> "FAILED", "date" -> date, "path" -> dest_path)))
}
