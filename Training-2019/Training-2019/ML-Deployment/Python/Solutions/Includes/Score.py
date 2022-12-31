# Databricks notebook source

'''
This is an example of the score.py file needed by Azure Drift Detection
'''

import pickle
import json
import numpy
import azureml.train.automl
from sklearn.externals import joblib
from sklearn.linear_model import Ridge
from azureml.core.model import Model
from azureml.core.run import Run
from azureml.monitoring import ModelDataCollector
import time
import pandas as pd


def init():
    global model, inputs_dc, prediction_dc, feature_names, categorical_features

    print("Model is initialized" + time.strftime("%H:%M:%S"))
    model_path = Model.get_model_path(model_name="airbnb2")
    model = joblib.load(model_path)

    feature_names = [
      'host_total_listings_count',
      'neighbourhood_cleansed',
      'zipcode',
      'latitude',
      'longitude',
      'property_type',
      'room_type',
      'accommodates',
      'bathrooms',
      'bedrooms',
      'beds',
      'bed_type',
      'minimum_nights',
      'number_of_reviews',
      'review_scores_rating',
      'review_scores_accuracy',
      'review_scores_cleanliness',
      'review_scores_checkin',
      'review_scores_communication',
      'review_scores_location',
      'review_scores_value']

    categorical_features = ["usaf", "wban", "p_k", "station_name"]

    inputs_dc = ModelDataCollector(model_name="driftmodel",
                                   identifier="inputs",
                                   feature_names=feature_names)

    prediction_dc = ModelDataCollector("driftmodel",
                                       identifier="predictions",
                                       feature_names=["temperature"])
    
    print("model initialized " + time.strftime("%H:%M:%S"))


def run(raw_data):
    global inputs_dc, prediction_dc

    try:
        data = json.loads(raw_data)["data"]
        data = pd.DataFrame(data)

        # Remove the categorical features as the model expects OHE values
        input_data = data.drop(categorical_features, axis=1)

        result = model.predict(input_data)

        # Collect the non-OHE dataframe
        collected_df = data[feature_names]

        inputs_dc.collect(collected_df.values)
        prediction_dc.collect(result)
        return result.tolist()
    except Exception as e:
        error = str(e)

        print(error + time.strftime("%H:%M:%S"))
        return error
