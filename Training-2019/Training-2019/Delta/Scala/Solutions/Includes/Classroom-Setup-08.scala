// Databricks notebook source
// MAGIC 
// MAGIC %python
// MAGIC module_name = "Time Travel"
// MAGIC spark.conf.set("com.databricks.training.module_name", module_name)
// MAGIC spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

// COMMAND ----------

// MAGIC %run ./Classroom-Setup

// COMMAND ----------

// MAGIC %run ./MLflow

// COMMAND ----------

def getOrCreateExperiment(experimentName:String):org.mlflow.tracking.MlflowContext = {
  import org.mlflow.tracking.ActiveRun
  import org.mlflow.tracking.MlflowContext
  import java.io.{File,PrintWriter}

  val context = new MlflowContext()
  val client = context.getClient()

  val experimentOpt = client.getExperimentByName(experimentName);
  if (!experimentOpt.isPresent()) {
    client.createExperiment(experimentName)
  }
  context.setExperimentName(experimentName)
  return context
}

displayHTML("""
<div>Declared module-specific method:</div>
<li>Declared <b style="color:green">getOrCreateMlflowRun()</b> to log experiments to MLflow</li>
""")

// COMMAND ----------

val basePath = userhome + "/delta-il08s"
cleanWorkspace(basePath)

displayHTML("""
<div>Declared module-specific resources:</div>
<li>Declared <b style="color:green">basePath:String</b> = <b style="color:blue">%s</b></li>
""".format(basePath))

// COMMAND ----------

cleanWorkspace(basePath)
displayHTML("Removed temp files from " + basePath)
