// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ##Working With Dates
// MAGIC 
// MAGIC Basic example of working dates. Getting the current date, casting dates and inserting load dates into a data frame.

// COMMAND ----------

// MAGIC %md
// MAGIC We have to import the java libraries for working with dates.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC import java.util.Calendar
// MAGIC import java.text.SimpleDateFormat;

// COMMAND ----------

// MAGIC %md
// MAGIC Convert a string date to date variable

// COMMAND ----------


val prundate = "2018-01-01"
val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
val rundate = format.parse(prundate)


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Get the current date and format it.

// COMMAND ----------

val rightnow = Calendar.getInstance().getTime()
val loaddateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val loaddate = loaddateformat.format(rightnow)