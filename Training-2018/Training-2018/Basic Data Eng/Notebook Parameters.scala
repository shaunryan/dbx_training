// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ##Notebook Parameters
// MAGIC 
// MAGIC When calling notebooks from data factory we may wish to pass parameters. This is achieved using a not very well named api called widgets.
// MAGIC 
// MAGIC Widgets allow you to pass in parameter values, they appear as text boxes, drop downs etc at the top of the notebook.
// MAGIC 
// MAGIC the following code adds a textbox parameter. Parameters for the text function are the parameter name, default value and label for UI
// MAGIC 
// MAGIC If you run this you will see a text box appear at the top of the notebook if there isn't one already. If it's there already then it does nothing.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC dbutils.widgets.text("rundate", "", "rundate (yyyy-MM-dd)")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Once widgets are added they aren't removed by simply removing the code above. The widget will remain. In order to remove parameters the following can be used.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC dbutils.widgets.removeAll()

// COMMAND ----------

// MAGIC %md
// MAGIC Add it back

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC dbutils.widgets.text("rundate", "2018-01-01", "rundate (yyyy-MM-dd)")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC A value can be returned from a parameter by using the following code. Note the result window prints the variable and it's type. Even though it's a date it is a string datatype and before it can be used as date it should type cast to data to check that it is a valid date. See the working with dates notebook

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val prundate = dbutils.widgets.get("rundate")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC We can test what the variable contains by simply running the following. So if you have a lot going in a script and the variable is set early on sometimes this is useful so that it appears at the bottom of the output.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC prundate