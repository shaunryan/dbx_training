// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <style>
// MAGIC   input { width: 10em }
// MAGIC </style>
// MAGIC 
// MAGIC <table>
// MAGIC   <tr style="background-color:#F0F0F0"><th colspan="2">What</th><th colspan="3">Value</th></tr>
// MAGIC   <tr>
// MAGIC     <td colspan="2">Storage Memory:</td>
// MAGIC     <td colspan="2"><input type="text"></td> 
// MAGIC   </tr>
// MAGIC   
// MAGIC   <tr style="background-color:#F0F0F0"><th colspan="2">Experiment</th><th>Min</th><th>Median</th><th>Max</th></tr>
// MAGIC 
// MAGIC   <tr>
// MAGIC     <td rowspan="2">#1: Not Cached</td>
// MAGIC     <td>Duration:</td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td>GC Time:</td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC   </tr>
// MAGIC 
// MAGIC   
// MAGIC   <tr>
// MAGIC     <td rowspan="2">#2: MEMORY_AND_DISK</td>
// MAGIC     <td>Duration:</td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td>GC Time:</td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC   </tr>
// MAGIC 
// MAGIC   <tr>
// MAGIC     <td rowspan="2">#3: DISK_ONLY</td>
// MAGIC     <td>Duration:</td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td>GC Time:</td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC   </tr>
// MAGIC 
// MAGIC   <tr>
// MAGIC     <td rowspan="2">#4: MEMORY_ONLY</td>
// MAGIC     <td>Duration:</td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td>GC Time:</td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC   </tr>
// MAGIC 
// MAGIC   <tr>
// MAGIC     <td rowspan="2">#5: MEMORY_ONLY_SER</td>
// MAGIC     <td>Duration:</td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td>GC Time:</td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC   </tr>
// MAGIC 
// MAGIC   <tr>
// MAGIC     <td rowspan="2">#6: MEMORY_AND_DISK_SER</td>
// MAGIC     <td>Duration:</td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td>GC Time:</td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC     <td><input type="text"></td>
// MAGIC   </tr>
// MAGIC 
// MAGIC </table>

// COMMAND ----------



// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>