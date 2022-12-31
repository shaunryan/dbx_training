// Databricks notebook source
// MAGIC %scala
// MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
// MAGIC 
// MAGIC //*******************************************
// MAGIC // GET VERSION OF APACHE SPARK
// MAGIC //*******************************************
// MAGIC 
// MAGIC // Get the version of spark
// MAGIC val Array(sparkMajorVersion, sparkMinorVersion, _) = spark.version.split("""\.""")
// MAGIC 
// MAGIC // Set the major and minor versions
// MAGIC spark.conf.set("com.databricks.training.spark.major-version", sparkMajorVersion)
// MAGIC spark.conf.set("com.databricks.training.spark.minor-version", sparkMinorVersion)
// MAGIC 
// MAGIC //*******************************************
// MAGIC // GET VERSION OF DATABRICKS RUNTIME
// MAGIC //*******************************************
// MAGIC 
// MAGIC // Get the version of the Databricks Runtime
// MAGIC val runtimeVersion = tags.collect({ case (t, v) if t.name == "sparkVersion" => v }).head
// MAGIC val runtimeVersions = runtimeVersion.split("""-""")
// MAGIC val (dbrVersion, scalaVersion) = if (runtimeVersions.size == 3) {
// MAGIC   val Array(dbrVersion, _, scalaVersion) = runtimeVersions
// MAGIC   (dbrVersion, scalaVersion.replace("scala", ""))
// MAGIC } else {
// MAGIC   val Array(dbrVersion, scalaVersion) = runtimeVersions
// MAGIC   (dbrVersion, scalaVersion.replace("scala", ""))
// MAGIC }
// MAGIC val Array(dbrMajorVersion, dbrMinorVersion, _) = dbrVersion.split("""\.""")
// MAGIC 
// MAGIC // Set the the major and minor versions
// MAGIC spark.conf.set("com.databricks.training.dbr.major-version", dbrMajorVersion)
// MAGIC spark.conf.set("com.databricks.training.dbr.minor-version", dbrMinorVersion)
// MAGIC 
// MAGIC //*******************************************
// MAGIC // GET USERNAME AND USERHOME
// MAGIC //*******************************************
// MAGIC 
// MAGIC // Get the user's name
// MAGIC val username = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
// MAGIC 
// MAGIC // Get the user's home. Create it if necessary
// MAGIC val userhome = s"dbfs:/user/$username"
// MAGIC // This call doesn't fail if it already exists
// MAGIC val created = dbutils.fs.mkdirs(userhome)
// MAGIC 
// MAGIC // Set the user's name and home directory
// MAGIC spark.conf.set("com.databricks.training.username", username)
// MAGIC spark.conf.set("com.databricks.training.userhome", userhome)
// MAGIC 
// MAGIC //**********************************
// MAGIC // VARIOUS UTILITY FUNCTIONS
// MAGIC //**********************************
// MAGIC 
// MAGIC def assertSparkVersion(expMajor:Int, expMinor:Int):String = {
// MAGIC   val major = spark.conf.get("com.databricks.training.spark.major-version")
// MAGIC   val minor = spark.conf.get("com.databricks.training.spark.minor-version")
// MAGIC 
// MAGIC   if ((major.toInt < expMajor) || (major.toInt == expMajor && minor.toInt < expMinor))
// MAGIC     throw new IllegalArgumentException(s"This notebook must be ran on Spark version $expMajor.$expMinor or better, found Spark $major.$minor")
// MAGIC 
// MAGIC   s"$major.$minor"
// MAGIC }
// MAGIC 
// MAGIC def requireDbrVersion(version:String):Unit = {
// MAGIC   val major = spark.conf.get("com.databricks.training.dbr.major-version")
// MAGIC   val minor = spark.conf.get("com.databricks.training.dbr.minor-version")
// MAGIC   val dbrVersion = major+"."+minor
// MAGIC   
// MAGIC   if (version != dbrVersion) {
// MAGIC     throw new IllegalArgumentException(s"This notebook must be ran on Databricks Runtime (DBR) $version version, found DBR $dbrVersion.")
// MAGIC   }
// MAGIC }
// MAGIC 
// MAGIC def assertDbrVersion(expMajor:Int, expMinor:Int):String = {
// MAGIC   val major = spark.conf.get("com.databricks.training.dbr.major-version")
// MAGIC   val minor = spark.conf.get("com.databricks.training.dbr.minor-version")
// MAGIC 
// MAGIC   if ((major.toInt < expMajor) || (major.toInt == expMajor && minor.toInt < expMinor))
// MAGIC     throw new IllegalArgumentException(s"This notebook must be ran on Databricks Runtime (DBR) version $expMajor.$expMinor or better, found DBR $major.$minor.")
// MAGIC   
// MAGIC   s"$major.$minor"
// MAGIC }
// MAGIC 
// MAGIC displayHTML(s"""
// MAGIC   <div>Successfully created class variables and utility functions.</div>
// MAGIC   <li><b>username</b>: $username</li>
// MAGIC   <li><b>userhome</b>: $userhome</li>
// MAGIC """)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC from __future__ import print_function
// MAGIC from __future__ import division
// MAGIC from pyspark.sql.functions import *
// MAGIC 
// MAGIC #**********************************
// MAGIC # VARIOUS UTILITY FUNCTIONS
// MAGIC #**********************************
// MAGIC 
// MAGIC def assertSparkVersion(expMajor, expMinor):
// MAGIC   major = spark.conf.get("com.databricks.training.spark.major-version")
// MAGIC   minor = spark.conf.get("com.databricks.training.spark.minor-version")
// MAGIC 
// MAGIC   if (int(major) < expMajor) or (int(major) == expMajor and int(minor) < expMinor):
// MAGIC     msg = "This notebook must run on Spark version {}.{} or better, found.".format(expMajor, expMinor, major, minor)
// MAGIC     raise Exception(msg)
// MAGIC 
// MAGIC   return major+"."+minor
// MAGIC 
// MAGIC def assertDbrVersion(expMajor, expMinor):
// MAGIC   major = spark.conf.get("com.databricks.training.dbr.major-version")
// MAGIC   minor = spark.conf.get("com.databricks.training.dbr.minor-version")
// MAGIC 
// MAGIC   if (int(major) < expMajor) or (int(major) == expMajor and int(minor) < expMinor):
// MAGIC     msg = "This notebook must run on Databricks Runtime (DBR) version {}.{} or better, found.".format(expMajor, expMinor, major, minor)
// MAGIC     raise Exception(msg)
// MAGIC     
// MAGIC   return major+"."+minor
// MAGIC 
// MAGIC #**********************************
// MAGIC # INIT VARIOUS VARIABLES
// MAGIC #**********************************
// MAGIC 
// MAGIC username = spark.conf.get("com.databricks.training.username")
// MAGIC userhome = spark.conf.get("com.databricks.training.userhome")
// MAGIC 
// MAGIC import sys
// MAGIC pythonVersion = spark.conf.set("com.databricks.training.python-version", sys.version[0:sys.version.index(" ")])
// MAGIC 
// MAGIC None # suppress output