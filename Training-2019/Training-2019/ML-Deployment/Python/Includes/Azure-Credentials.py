# Databricks notebook source

# Cosmos DB
dbutils.widgets.text("Cosmos URI", "https://airbnbprediction.documents.azure.com:443/") 
dbutils.widgets.text("Cosmos Database", "predictions") 
dbutils.widgets.text("Cosmos Container", "predictions") 
dbutils.widgets.text("Cosmos Read-Only Key", "YN7YcizZ3Q06UadXdESpsFvaZ1TJUqxBe4wXpH8A6eJUoMFe41kLSBbQx4yxwxbtdtLcDlvfkvHuO7la0XeV5A==")  # Databricks can give you read-only access to our Cosmos database
dbutils.widgets.text("Cosmos Read-Write Key", "") # You will need to set up your Cosmos database for write access.  Paste your keys here


# Event Hub
dbutils.widgets.text("EventHub Name", "airbnb-data") 
dbutils.widgets.text("EventHub Connection String", "") 
dbutils.widgets.text("Subscription ID", "") 

# COMMAND ----------

cosmos_uri = dbutils.widgets.get("Cosmos URI")
cosmos_read_key = dbutils.widgets.get("Cosmos Read-Only Key")
cosmos_read_write_key = dbutils.widgets.get("Cosmos Read-Write Key")
cosmos_database = dbutils.widgets.get("Cosmos Database")
cosmos_container = dbutils.widgets.get("Cosmos Container")

event_hub_name = dbutils.widgets.get("EventHub Name")
connection_string =  dbutils.widgets.get("EventHub Connection String") + ";EntityPath=" + event_hub_name
subscription_id = dbutils.widgets.get("Subscription ID")

# COMMAND ----------

# dbutils.widgets.removeAll()
