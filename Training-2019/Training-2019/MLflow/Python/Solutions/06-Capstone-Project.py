# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Capstone Project: Managing the Machine Learning Lifecycle
# MAGIC 
# MAGIC Create a workflow that includes pre-processing logic, the optimal ML algorithm and hyperparameters, and post-processing logic.
# MAGIC 
# MAGIC ## Instructions
# MAGIC 
# MAGIC In this course, we've primarily used Random Forest in `sklearn` to model the Airbnb dataset.  In this exercise, perform the following tasks:
# MAGIC <br><br>
# MAGIC 0. Create custom pre-processing logic to featurize the data
# MAGIC 0. Try a number of different algorithms and hyperparameters.  Choose the most performant solution
# MAGIC 0. Create related post-processing logic
# MAGIC 0. Package the results and execute it as its own run

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Clear the project directory in case you have lingering files from other runs.  Create a fresh directory.  Use this throughout this notebook.

# COMMAND ----------

project_path = userhome+"/ml-production/Capstone/"

dbutils.fs.rm(project_path, True)
dbutils.fs.mkdirs(project_path)

print("Created directory: {}".format(project_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-processing
# MAGIC 
# MAGIC Take a look at the dataset and notice that there are plenty of strings and `NaN` values present. Our end goal is to train a sklearn regression model to predict the price of an airbnb listing.
# MAGIC 
# MAGIC 
# MAGIC Before we can start training, we need to pre-process our data to be compatible with sklearn models by making all features purely numerical. 

# COMMAND ----------

import pandas as pd

airbnbDF = spark.read.parquet("/mnt/training/airbnb/sf-listings/sf-listings-correct-types.parquet").toPandas()
display(airbnbDF)

# COMMAND ----------

# MAGIC %md
# MAGIC In the following cells we will walk you through the most basic pre-processing step necessary. Feel free to add additional steps afterwards to improve your model performance.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC First, convert the `price` from a string to a float since the regression model will be predicting numerical values.

# COMMAND ----------

# ANSWER
airbnbDF["int_price"] = airbnbDF["price"].apply(lambda s: float(s.replace("$", "").replace(",", "")))
airbnbDF_cleaned_price = airbnbDF.drop(["price"],axis=1)

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at our remaining columns with strings (or numbers) and decide if you would like to keep them as features or not.
# MAGIC 
# MAGIC Remove the features you decide not to keep.

# COMMAND ----------

# ANSWER
airbnbDF_cleaned_features = airbnbDF_cleaned_price.drop(["host_is_superhost", "instant_bookable","cancellation_policy"], axis=1)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC For the string columns that you've decided to keep, pick a numerical encoding for the string columns. Don't forget to deal with the `NaN` entries in those columns first.

# COMMAND ----------

# ANSWER

airbnbDF_cleaned_features = airbnbDF_cleaned_features[airbnbDF_cleaned_features["zipcode"] != "-- default zip code --"] # removed entry with unusual zipcode
airbnbDF_cleaned_features = airbnbDF_cleaned_features.dropna(subset=["zipcode"])

# encoded each string label into an integer
airbnbDF_cleaned_features['zipcode'] = pd.factorize(airbnbDF_cleaned_features['zipcode'])[0]
airbnbDF_cleaned_features['neighbourhood_cleansed'] = pd.factorize(airbnbDF_cleaned_features['neighbourhood_cleansed'])[0]
airbnbDF_cleaned_features['property_type'] = pd.factorize(airbnbDF_cleaned_features['property_type'])[0]
airbnbDF_cleaned_features['room_type'] = pd.factorize(airbnbDF_cleaned_features['room_type'])[0]
airbnbDF_cleaned_features['bed_type'] = pd.factorize(airbnbDF_cleaned_features['bed_type'])[0]


# COMMAND ----------

# MAGIC %md
# MAGIC Before we create a train test split, check that all your columns are numerical. Remember to drop the original string columns after creating numerical representations of them.
# MAGIC 
# MAGIC Make sure to drop the price column from the training data when doing the train test split.

# COMMAND ----------

# ANSWER
from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(airbnbDF_cleaned_features.drop(["int_price"], axis=1), airbnbDF_cleaned_features[["int_price"]].values.ravel(), random_state=42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model
# MAGIC 
# MAGIC After cleaning our data, we can start creating our model!

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Firstly, if there are still `NaN`'s in your data, you may want to impute these values instead of dropping those entries entirely. Make sure that any further processing/imputing steps after the train test split is part of a model/pipeline that can be saved.
# MAGIC 
# MAGIC In the following cell, create and fit a single sklearn model.

# COMMAND ----------

# ANSWER

import numpy as np
from sklearn.preprocessing import Imputer
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline

# impute NaN values of data with median
columns_to_impute = ["review_scores_value", "review_scores_location", "review_scores_rating", "review_scores_accuracy", "review_scores_cleanliness", "review_scores_checkin", "review_scores_communication", "host_total_listings_count", "bathrooms", "beds"]
preprocessing_steps = []
for col in columns_to_impute:
  imp = SimpleImputer(missing_values=np.nan, strategy='median')
  preprocessing_steps.append((col+"Imputer", imp))

# define regression 
n_estimators=100
max_depth=5
model = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth) 

# create and train pipeline
pipeline = Pipeline(preprocessing_steps+[("model", model)])
pipeline.fit(X_train, y_train) 


# COMMAND ----------

# MAGIC %md
# MAGIC Pick and calculate a regression metric for evaluating your model.

# COMMAND ----------

# ANSWER
import numpy as np
from sklearn.metrics import mean_squared_error

pipeline.predict(X_test)

rmse = np.sqrt(mean_squared_error(y_test, pipeline.predict(X_test)))
rmse

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Log your model on MLflow with the same metric you calculated above so we can compare all the different models you have tried! Make sure to also log any hyperparameters that you plan on tuning!

# COMMAND ----------

# ANSWER
import mlflow.sklearn

with mlflow.start_run() as run:
  mlflow.sklearn.log_model(pipeline, "model")
  mlflow.log_param("max_depth", max_depth)
  mlflow.log_param("n_estimators", n_estimators)
  mlflow.log_metric("rmse", rmse)

  experimentID = run.info.experiment_id

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Change and re-run the above 3 code cells to log different models and/or models with different hyperparameters until you are satisfied with the performance of at least 1 of them.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Look through the MLflow UI for the best model. Copy its `URI` so you can load it as a `pyfunc` model.

# COMMAND ----------

# ANSWER
import mlflow.pyfunc
from mlflow.tracking import MlflowClient
import pandas as pd
client = MlflowClient()

runs = []
for run in client.search_runs(experimentID):
  run_dict = run.to_dictionary()
  rmse = run_dict["data"]["metrics"].get("rmse")
  artifact_uri = run_dict["info"].get("artifact_uri")
  if rmse:
    runs.append((rmse, artifact_uri))
  
runsDF = pd.DataFrame(runs, columns = ["rmse", "artifact_uri"])
best_URI = runsDF.sort_values("rmse").iloc[0,-1]

best_model = mlflow.pyfunc.load_model(model_uri=(best_URI).replace("dbfs:", "/dbfs") + "/model")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-processing
# MAGIC 
# MAGIC Our model currently gives us the predicted price per night for each Airbnb listing. Now we would like our model to tell us what the price per person would be for each listing, assuming the number of renters is equal to the `accommodates` value. 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Fill in the following model class to add in a post-processing step which will get us from total price per night to **price per person per night**.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Check out <a href="https://www.mlflow.org/docs/latest/models.html#id13" target="_blank">the MLFlow docs for help.</a>

# COMMAND ----------

# ANSWER
class Airbnb_Model(mlflow.pyfunc.PythonModel):

    def __init__(self, model):
        self.model = model

    def postprocess_result(self, model_input, results):
        return results/list(model_input["accommodates"])
    
    def predict(self, context, model_input):
        results = self.model.predict(model_input)
        return self.postprocess_result(model_input, results)

# COMMAND ----------

# MAGIC %md
# MAGIC Construct and save the model to the given `final_model_path`.

# COMMAND ----------

# ANSWER
final_model_path =  project_path.replace("dbfs:", "/dbfs") + "model"

price_per_person = Airbnb_Model(best_model)
mlflow.pyfunc.save_model(path=final_model_path, python_model=price_per_person)

# COMMAND ----------

# MAGIC %md
# MAGIC Load the model in `python_function` format and apply it to our test data `X_test` to check that we are getting price per person predictions now.

# COMMAND ----------

# ANSWER
# Load the model
final_model = mlflow.pyfunc.load_model(final_model_path)

# Apply the model
final_model.predict(X_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Packaging your Model
# MAGIC 
# MAGIC Now we would like to package our completed model! 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC First save your testing data at `test_data_path` so we can test the packaged model.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** When using `.to_csv` make sure to set `index=False` so you don't end up with an extra index column in your saved dataframe.

# COMMAND ----------

# ANSWER
# save the testing data 
test_data_path = project_path.replace("dbfs:", "/dbfs") + "test_data.csv"
X_test.to_csv(test_data_path, index=False)

prediction_path = project_path.replace("dbfs:", "/dbfs") + "predictions.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC First we will determine what the project script should do. Fill out the `model_predict` function to load out the trained model you just saved (at `final_model_path`) and make price per person predictions on the data at `test_data_path`. Then those predictions should be saved under `prediction_path` for the user to access later.
# MAGIC 
# MAGIC Run the cell to check that your function is behaving correctly and that you have predictions saved at `demo_prediction_path`.

# COMMAND ----------

# ANSWER
import click
import mlflow.pyfunc
import pandas as pd

@click.command()
@click.option("--final_model_path", default="", type=str)
@click.option("--test_data_path", default="", type=str)
@click.option("--prediction_path", default="", type=str)
def model_predict(final_model_path, test_data_path, prediction_path):
    final_model = mlflow.pyfunc.load_model(final_model_path)
    X_test = pd.read_csv(test_data_path)
    prediction = final_model.predict(X_test) 
    pd.DataFrame(prediction).to_csv(prediction_path, index=False)

# test model_predict function
demo_prediction_path = project_path.replace("dbfs:", "/dbfs") + "demo_predictions.csv"

from click.testing import CliRunner
runner = CliRunner()
result = runner.invoke(model_predict, ['--final_model_path', final_model_path, 
                                       '--test_data_path', test_data_path,
                                       '--prediction_path', demo_prediction_path], catch_exceptions=True)

assert result.exit_code == 0, "Code failed" # Check to see that it worked
print("Price per person predictions: ")
print(pd.read_csv(demo_prediction_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we will create a MLproject file and put it under our `project_path`. Complete the parameters and command of the file.

# COMMAND ----------

# ANSWER
dbutils.fs.put(project_path + "MLproject", 
'''
name: Capstone-Project

conda_env: conda.yaml

entry_points:
  main:
    parameters:
      final_model_path: {type: str, default: ""}
      test_data_path: {type: str, default: ""}
      prediction_path: {type: str, default: ""}
    command: "python predict.py --test_data_path {test_data_path} --final_model_path {final_model_path} --prediction_path {prediction_path}"
'''.strip(), overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC We then create a `conda.yaml` file to list the dependencies needed to run our script.

# COMMAND ----------

dbutils.fs.put(project_path + "conda.yaml", 
'''
name: Capstone
channels:
  - defaults
dependencies:
  - cloudpickle=0.8.0
  - numpy=1.16.2
  - pandas=0.23.0
  - scikit-learn=0.20.3
  - pip:
    - mlflow==1.0.0
'''.strip(), overwrite=True)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You can check the versions match your current environment using the following cell.

# COMMAND ----------

import cloudpickle
print("cloudpickle: " + cloudpickle.__version__)
import numpy
print("numpy: " + numpy.__version__)
import pandas
print("pandas: " + pandas.__version__)
import sklearn
print("sklearn: " + sklearn.__version__)
import mlflow
print("mlflow: " + mlflow.__version__)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we will put the `predict.py` script into our project package. Complete the `.py` file by copying and placing the `model_predict` function you defined above.

# COMMAND ----------

# ANSWER
dbutils.fs.put(project_path + "predict.py", 
'''
import click
import mlflow.pyfunc
import pandas as pd

@click.command()
@click.option("--final_model_path", default="", type=str)
@click.option("--test_data_path", default="", type=str)
@click.option("--prediction_path", default="", type=str)
def model_predict(final_model_path, test_data_path, prediction_path):
    final_model = mlflow.pyfunc.load_model(final_model_path)
    X_test = pd.read_csv(test_data_path)
    prediction = final_model.predict(X_test) 
    pd.DataFrame(prediction).to_csv(prediction_path, index = False)
    
if __name__ == "__main__":
  model_predict()

'''.strip(), overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's double check all the files we've created are in the `project_path` folder. You should have at least the following 3 files:
# MAGIC * `MLproject`
# MAGIC * `conda.yaml`
# MAGIC * `predict.py`

# COMMAND ----------

dbutils.fs.ls(project_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Under `project_path` is your completely packaged project. Run the project to use the model saved at `final_model_path` to predict the price per person of each Airbnb listing in `test_data_path` and save those predictions under `prediction_path`.

# COMMAND ----------

# ANSWER
mlflow.projects.run(project_path.replace("dbfs:", "/dbfs"),
  parameters={
    "final_model_path": final_model_path,
    "test_data_path": test_data_path,
    "prediction_path": prediction_path
})

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to check that your model's predictions are there!

# COMMAND ----------

print("Price per person predictions: ")
print(pd.read_csv(prediction_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following command to clear the project and data files from your directory.

# COMMAND ----------

dbutils.fs.rm(project_path, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## IMPORTANT Next Steps
# MAGIC * Please complete the <a href="https://www.surveymonkey.com/r/7P83D9N" target="_blank">short feedback survey</a>.  Your input is extremely important and shapes future course development.
# MAGIC * Congratulations, you have completed the course!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>