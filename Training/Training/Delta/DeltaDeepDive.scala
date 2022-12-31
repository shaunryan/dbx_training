// Databricks notebook source
// MAGIC %run AzureDataFactory/Includes/SetupEnvironment

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC create table shaun_ryan_gocompare_com_dbs.deltademo
// MAGIC (
// MAGIC   ID string comment 'this is the ID',
// MAGIC   Created Timestamp not null comment 'this is the time'
// MAGIC )
// MAGIC using delta

// COMMAND ----------

// MAGIC %fs ls dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/_delta_log/

// COMMAND ----------

// MAGIC %fs head dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/_delta_log/00000000000000000000.crc

// COMMAND ----------

// MAGIC %fs head dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/_delta_log/00000000000000000000.json

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC alter table shaun_ryan_gocompare_com_dbs.deltademo ADD COLUMNS (newcol1 INT AFTER ID)

// COMMAND ----------

// MAGIC %fs ls dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/_delta_log/

// COMMAND ----------

// MAGIC %fs head dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/_delta_log/00000000000000000001.crc

// COMMAND ----------

// MAGIC %fs head dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/_delta_log/00000000000000000001.json	

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into shaun_ryan_gocompare_com_dbs.deltademo
// MAGIC select
// MAGIC   uuid() as ID,
// MAGIC   1 as newcol1,
// MAGIC   now() as Created

// COMMAND ----------

// MAGIC %fs ls dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/_delta_log/

// COMMAND ----------

// MAGIC %fs head dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/_delta_log/00000000000000000002.crc

// COMMAND ----------

// MAGIC %fs head dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/_delta_log/00000000000000000002.json

// COMMAND ----------

// MAGIC %fs ls dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/

// COMMAND ----------

val data = sqlContext.read.parquet("dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/part-00000-aca9d5a1-da91-44da-976e-f8c594a59ce0-c000.snappy.parquet")

// COMMAND ----------

import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.expressions._

def randomInt1to100 = scala.util.Random.nextInt(100)+1

val df = sc.parallelize(
  Seq.fill(10000000){(randomInt1to100)}
).toDF("newcol1")
 .withColumn("ID", expr("uuid()"))
 .withColumn("Created", expr("now()"))
 .select($"ID",$"newcol1",$"Created")

df.write.insertInto("shaun_ryan_gocompare_com_dbs.deltademo")

// COMMAND ----------

// MAGIC %fs ls dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/

// COMMAND ----------

// MAGIC %fs ls dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/_delta_log

// COMMAND ----------

// MAGIC %fs head dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/_delta_log/00000000000000000003.crc

// COMMAND ----------

// MAGIC %fs head dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/_delta_log/00000000000000000003.json

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC delete from shaun_ryan_gocompare_com_dbs.deltademo where newcol1 = 1

// COMMAND ----------

// MAGIC %fs ls dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/

// COMMAND ----------

// MAGIC %fs ls dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/_delta_log/

// COMMAND ----------

// MAGIC %fs head dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/_delta_log/00000000000000000004.crc

// COMMAND ----------

// MAGIC %fs head dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/_delta_log/00000000000000000004.json

// COMMAND ----------

// MAGIC %sql
// MAGIC optimize shaun_ryan_gocompare_com_dbs.deltademo

// COMMAND ----------

// MAGIC %fs ls dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/

// COMMAND ----------

// MAGIC %fs head dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/_delta_log/00000000000000000005.crc

// COMMAND ----------

// MAGIC %fs head dbfs:/user/hive/warehouse/shaun_ryan_gocompare_com_dbs.db/deltademo/_delta_log/00000000000000000005.json

// COMMAND ----------

// MAGIC %sql
// MAGIC VACUUM shaun_ryan_gocompare_com_dbs.deltademo