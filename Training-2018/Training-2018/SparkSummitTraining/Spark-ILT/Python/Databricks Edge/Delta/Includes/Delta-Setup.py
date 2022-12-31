# Databricks notebook source
# MAGIC %python
# MAGIC spark.sql("set spark.databricks.delta.preview.enabled=true")
# MAGIC spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled=false")
# MAGIC None # suppress output

# COMMAND ----------

def removeFolderContents(folder):
  for fileInfo in dbutils.fs.ls(folder):
    dbutils.fs.rm(fileInfo.path, true)
None

# COMMAND ----------

def stopStreamAfterTime(streamingQuery, hours=0.0, minutes=0.0, seconds=0.0):
  """
    Stops a streaming query after the specified number of minutes.
    This helps to avoid unintentionally leaving the streaming job running overnight.
    If time<=0, the default is 30 minutes.
  """
  from threading import Timer
  seconds=(hours*60.0+minutes)*60.0+seconds
  if (seconds < 0.000001):
    seconds=30*60 #default to 30 minutes
  timer=Timer(seconds, streamingQuery.stop)
  timer.start()
None

# COMMAND ----------

def stopAllStreamsAfterTime(hours=0.0, minutes=0.0, seconds=0.0):
  """
    Stops all streaming querying in this notebook after the specified number of hours.
    This helps to avoid unintentionally leaving the streaming job running overnight.
    If time<=0, the default is 2 hours.
  """
  from threading import Timer
  def stopAllStreams():
    for streamingQuery in spark.streams.active:
      streamingQuery.stop()
  try:
    if topAllStreamsAfterTime.timer.is_alive():
      stopAllStreamsAfterTime.timer.cancel()
  except:
    pass
  seconds=(hours*60.0+minutes)*60.0+seconds
  if (seconds < 0.000001):
    seconds=2*60*60 #default to 2 hours
  stopAllStreamsAfterTime.timer=Timer(seconds, stopAllStreams)
  stopAllStreamsAfterTime.timer.start()

stopAllStreamsAfterTime()

None