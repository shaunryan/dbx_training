# Databricks notebook source
# We know these numbers from the previous lab
expectedFast14 = "413199362.16"
expectedFast17 = "492940869.09"

expectedSlow14 = "4132674423.72"
expectedSlow17 = "4923864114.44"

def validateSchema(df):
  assert __builtin__.len(df.columns) == 3, "Expected three and only three columns"

  schema = str(df.schema)
  assert "year,IntegerType" in schema, "Missing the year column"
  assert "day,IntegerType" in schema, "Missing the day column"
  assert "amount,DecimalType" in schema, "Missing the amount column"
  
  expected = 61
  total = df.count()

  assert total == expected, "Expected {} records, found {}".format(expected, total)

def validateSum(df, expected):
  from pyspark.sql.functions import sum
  total = df.select( sum(col("amount")).cast("decimal(20,2)").cast("string").alias("total") ).first()[0]
  assert total == expected, "Expected the final sum to be {} but found {}".format(expected, total)