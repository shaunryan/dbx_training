// Databricks notebook source
// MAGIC %python
// MAGIC spark.sql("set spark.databricks.delta.preview.enabled=true")
// MAGIC spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled=false")
// MAGIC None # suppress output

// COMMAND ----------

import org.apache.spark.sql.streaming.StreamingQuery

def killQueryAndPrint(streamingQuery:StreamingQuery){
  print("Killing Stream "+System.currentTimeMillis / 1000)
  streamingQuery.stop()
}

def stopStreamAfterTime(streamingQuery:StreamingQuery, hours:Double = 0.0, minutes:Double = 0.0, seconds:Double = 0.0) {
  import java.util.{Timer, TimerTask}
  /*
    Stops a streaming query after the specified number of minutes.
    This helps to avoid unintentionally leaving the streaming job running overnight.
    If time<=0, the default is 30 minutes.
  */
  var delay = (hours * 60.0 + minutes) * 60.0 + seconds
  if (delay < 0.000001) {
    delay = 30 * 60 //default to 30 minutes
  }
  
  val timer: Timer = new Timer()
  val task = new TimerTask {
    override def run(): Unit =  killQueryAndPrint(streamingQuery)
  }
  println("Scheduling Kill Stream: "+System.currentTimeMillis / 1000)
  timer.schedule(task, 1000 * delay.toLong)
  
}

def removeFolderContents(folder: String) = {  
  for (fileInfo <- dbutils.fs.ls(folder)) {
    dbutils.fs.rm(fileInfo.path, true)
}}

displayHTML("Imported Delta-specific logic...") // suppress output