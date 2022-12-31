# Databricks notebook source
# MAGIC 
# MAGIC %python
# MAGIC def dbTest(id, expected, result):
# MAGIC   try:
# MAGIC     ok = spark.conf.get("com.databricks.training.valid") == "true"
# MAGIC   except:
# MAGIC     ok = False
# MAGIC   if not ok:
# MAGIC     raise Exception("You are not authorized to run this course.")
# MAGIC   assert str(expected) == str(result), "{} does not equal expected {}".format(result, expected)

# COMMAND ----------

# MAGIC %scala
# MAGIC def dbTest[T](id: String, expected: T, result: => T, message: String = ""): Unit = {
# MAGIC   val ok = try {
# MAGIC     spark.conf.get("com.databricks.training.valid") == "true"
# MAGIC   }
# MAGIC   catch {
# MAGIC     case _: Exception => false
# MAGIC   }
# MAGIC   if (! ok) throw new Exception("You are not authorized to run this course.")
# MAGIC   assert(result == expected, message)
# MAGIC }
# MAGIC displayHTML("Imported Test Library...") // suppress output