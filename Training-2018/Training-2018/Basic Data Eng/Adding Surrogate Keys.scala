// Databricks notebook source
// MAGIC %md
// MAGIC ##Adding Surrogate Keys
// MAGIC 
// MAGIC Like most big data tech stacks. Automatically incrementing identities aren't supported.
// MAGIC 
// MAGIC The best technique I've found so far is take the last max key and add to a row number of the data set.
// MAGIC The row number is acheived in a very similar way as SQL Server - by using a window function over an arbitrary or default partition and current natural order.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC The max key can be taken from a table as follows.
// MAGIC 
// MAGIC The aggregate SQL function returns a Row object from which we can use an array index of 0 to return the first column value.
// MAGIC Coincidentally if there we more columns you can access the value respectively using the 0 bound ordinal of the column... e.g. 0, 1, 2, 3...
// MAGIC 
// MAGIC Note here the use of val and var. Val variables are immutable, they cannot change. Var variables can change.
// MAGIC Because we want to re-assign the keyseed after checking to see if there is data we need to use var not val!
// MAGIC 
// MAGIC Note: in this case this is fine since it's a scalar variable. Generally with distributed data processing however we aim to use immutable variables (val)

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC var keyseed = 0L //L on the end since we want a BigInt or Long data type
// MAGIC 
// MAGIC //to get the max key value it's easy and convenient to use spark SQL
// MAGIC //first ensures we get a Row object and not a Data Frame
// MAGIC val maxkeyrow = spark.sql("SELECT MAX(Key) FROM Energy.GeneralReference").first 
// MAGIC 
// MAGIC //If there is no data the row will be a null sequence / array... so we need to check this and default to 0
// MAGIC if (maxkeyrow.toString() != "[null]")
// MAGIC {
// MAGIC    keyseed = maxkeyrow.getLong(0) //get the first column value. If we wanted the second string column value we could do this maxkeyrow.getString(1)
// MAGIC }

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC This next section of code creates a dataframe and adds a surrogate key using a window function

// COMMAND ----------


//We have to import the functions library to use the lit() function
import org.apache.spark.sql.functions._

//note that ._ on the end of a library means import all the class (object definitions) in that library
//we could just import them explicitly e.g.
import org.apache.spark.sql.expressions.Window
//the following would also work, but imports all the classes in the library not just Window. Less code but can be more overhead.
//import org.apache.spark.sql.expressions._

//get a Data Frame that we can add a surrogate key to
val sqlDF = spark.sql("SELECT EnumId, ThirdPartyId, ReferenceType, Value FROM Energy.GeneralReference")

//The lit() function declares a literal column. Dataframes work with column objects that are functional expressions.
//Therefore we need a function to declare a literal column; this function is lit() e.g. lit(10), lit("A new column")
//This is an arbitrary low cost technique here since we don't care about the order or the window function we just need it so we can use row_number().over()
val windowSpecKey = Window.orderBy(lit(1).asc)

//use withColumn to create a new key column and add (+) the literal seed key to the rownumber over the window function
//note transforms have to be assigned to a new dataframe since they are immutable (declared with val)
val sqlDFWithKey = sqlDF
  .withColumn("Key", (lit(keyseed) + row_number().over(windowSpecKey)))
  //this will append the column to the right of the data frame. We can select the order expicitly to re-order columns
  .select($"Key", $"EnumId", $"ThirdPartyId", $"ReferenceType", $"Value")
  //note the $ is a shorthand way of doing the following which is identical but more typing and more brackets to go wrong
  .select(column("Key"), column("EnumId"), column("ThirdPartyId"), column("ReferenceType"), column("Value"), lit("a new column").as("LitExample"))
  //remember that dataframe columns are functions or expressions. column() is a function that returns a column value from it's name
  //this is fundamental and very powerful because a column can be any kind of function and therefore can be any kind of transformation that we need to do
  //see the last column where we've shown another example way of adding a column and using lit directly as an expression that also gets aliased

//check the output without writing the data anywhere
display(sqlDFWithKey)