# Databricks notebook source

'''
This script sets up MLflow and handles the case that it is executed
by Databricks' automated testing server
'''

try:
  import os
  import mlflow
  from mlflow.tracking import MlflowClient
  from databricks_cli.configure.provider import get_config
  from mlflow.exceptions import RestException
  
  os.environ['DATABRICKS_HOST'] = get_config().host
  os.environ['DATABRICKS_TOKEN'] = get_config().token
  tags = sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(dbutils.entry_point.getDbutils().notebook().getContext().tags())
  
  if tags.get("notebookId"):
    os.environ["MLFLOW_AUTODETECT_EXPERIMENT_ID"] = 'true'
  else:
    # Handles notebooks run by test server (executed as run)
    _name = "/Shared/BuildExperiment"
    
    try:
      client = MlflowClient()
      experiment = client.get_experiment_by_name("/Shared/BuildExperiment")

      if experiment:
        client.delete_experiment(experiment.experiment_id) # Delete past runs if possible
        
      mlflow.create_experiment(_name)
    except RestException: # experiment already exists
      pass
    
    os.environ['MLFLOW_EXPERIMENT_NAME'] = _name
  
  # Silence YAML deprecation issue https://github.com/yaml/pyyaml/wiki/PyYAML-yaml.load(input)-Deprecation
  os.environ["PYTHONWARNINGS"] = 'ignore::yaml.YAMLLoadWarning' 

  displayHTML("Initialized environment variables for MLflow server...")
except ImportError:
  raise ImportError ("Attach the MLflow library to your cluster through the clusters tab in order to run this courseware.  You cannot continue without this...")
  
