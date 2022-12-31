// Databricks notebook source
// We know these numbers from the previous lab
val expectedFast14 = "413199362.16"
val expectedFast17 = "492940869.09"

val expectedSlow14 = "4132674423.72"
val expectedSlow17 = "4923864114.44"

def validateSchema(df:DataFrame):Unit = {
  assert(df.columns.size == 3, "Expected three and only three columns")

  val schema = df.schema.mkString
  assert(schema.contains("year,IntegerType"), "Missing the year column")
  assert(schema.contains("day,IntegerType"), "Missing the day column")
  assert(schema.contains("amount,DecimalType"), "Missing the amount column")
  
  val expected = 61
  val total = df.count()

  assert(total == expected, "Expected %s records, found %s".format(expected, total))
}

def validateSum(df:DataFrame, expected:String):Unit = {
  import org.apache.spark.sql.functions.sum
  val total = df.select( sum($"amount").cast("decimal(20,2)").cast("string").as("total") ).as[String].first
  assert(total == expected, "Expected the final sum to be %s but found %s".format(expected, total))
}