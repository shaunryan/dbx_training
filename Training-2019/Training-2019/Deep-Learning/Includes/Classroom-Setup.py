# Databricks notebook source
# MAGIC 
# MAGIC %python
# MAGIC course_name="Deep-Learning"
# MAGIC 
# MAGIC import warnings
# MAGIC warnings.filterwarnings("ignore")
# MAGIC 
# MAGIC import tensorflow
# MAGIC tensorflow.logging.set_verbosity(tensorflow.logging.ERROR)  
# MAGIC 
# MAGIC 
# MAGIC None # Suppress output

# COMMAND ----------

def display_run_uri(experiment_id, run_id):
    host_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").get()
    uri = "https://{}/#mlflow/experiments/{}/runs/{}".format(host_name,experiment_id,run_id)
    displayHTML("""<b>Run URI:</b> <a href="{}">{}</a>""".format(uri,uri))

# COMMAND ----------

# MAGIC %run "./Dataset-Mounts"

# COMMAND ----------

# assertIsMlRuntime()

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC ml_username = username.replace("+", "")
# MAGIC # Make sure the directory exists first.
# MAGIC dbutils.fs.mkdirs("dbfs:/user/" + username)
# MAGIC dbutils.fs.mkdirs("dbfs:/user/" + username + "/temp")
# MAGIC dbutils.fs.mkdirs("dbfs:/user/" + username + "/deep-learning")
# MAGIC dbutils.fs.mkdirs("dbfs:/ml/" + ml_username) # Optimized fuse mount
# MAGIC 
# MAGIC def waitForMLflow():
# MAGIC   try:
# MAGIC     import mlflow; print("""The module "mlflow" is attached and ready to go.""");
# MAGIC   except ModuleNotFoundError:
# MAGIC     print("""The module "mlflow" is not yet attached to the cluster, waiting...""");
# MAGIC     while True:
# MAGIC       try: import mlflow; print("""The module "mlflow" is attached and ready to go."""); break;
# MAGIC       except ModuleNotFoundError: import time; time.sleep(1); print(".", end="");
# MAGIC 
# MAGIC def waitForShap():
# MAGIC   try:
# MAGIC     import shap; print("""The module "shap" is attached and ready to go.""");
# MAGIC   except ModuleNotFoundError:
# MAGIC     print("""The module "shap" is not yet attached to the cluster, waiting...""");
# MAGIC     while True:
# MAGIC       try: import shap; print("""The module "shap" is attached and ready to go."""); break;
# MAGIC       except ModuleNotFoundError: import time; time.sleep(1); print(".", end="");
# MAGIC         
# MAGIC def waitForLime():
# MAGIC   try:
# MAGIC     import lime; print("""The module "lime" is attached and ready to go.""");
# MAGIC   except ModuleNotFoundError:
# MAGIC     print("""The module "lime" is not yet attached to the cluster, waiting...""");
# MAGIC     while True:
# MAGIC       try: import lime; print("""The module "lime" is attached and ready to go."""); break;
# MAGIC       except ModuleNotFoundError: import time; time.sleep(1); print(".", end="");
# MAGIC 
# MAGIC None # Suppress output

# COMMAND ----------

# MAGIC %scala
# MAGIC displayHTML("All done!")

# COMMAND ----------

from sklearn.metrics import confusion_matrix,f1_score,accuracy_score,fbeta_score,precision_score,recall_score
import matplotlib.pyplot as plt
import numpy as np

from sklearn.utils.multiclass import unique_labels

def plot_confusion_matrix(y_true, y_pred, classes,
                          title=None,
                          cmap=plt.cm.Blues):
    # Compute confusion matrix
    cm = confusion_matrix(y_true, y_pred)
    fig, ax = plt.subplots()
    im = ax.imshow(cm, interpolation='nearest', cmap=cmap)
    ax.figure.colorbar(im, ax=ax)
    ax.set(xticks=np.arange(cm.shape[1]),
           yticks=np.arange(cm.shape[0]),
           xticklabels=classes, yticklabels=classes,
           title=title,
           ylabel='True label',
           xlabel='Predicted label')

    plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
             rotation_mode="anchor")

    fmt = 'd'
    thresh = cm.max() / 2.
    for i in range(cm.shape[0]):
        for j in range(cm.shape[1]):
            ax.text(j, i, format(cm[i, j], fmt),
                    ha="center", va="center",
                    color="white" if cm[i, j] > thresh else "black")
    fig.tight_layout()
    return fig

np.set_printoptions(precision=2)
