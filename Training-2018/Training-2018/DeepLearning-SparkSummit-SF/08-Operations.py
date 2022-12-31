# Databricks notebook source
# MAGIC %md # Operations in the Real World
# MAGIC 
# MAGIC ## Practical Options, Tools, Patterns, and Considerations for Deep Learning

# COMMAND ----------

# MAGIC %md There are various ways to use deep learning in an enterprise setting that may not require designing your own networks!
# MAGIC 
# MAGIC ### Ways to Use Deep Learning
# MAGIC 
# MAGIC (in order from least complex/expensive investment to most)
# MAGIC 
# MAGIC [1] Load and use a pretrained model
# MAGIC 
# MAGIC Many of the existing toolkit projects offer models pretrained on datasets, including
# MAGIC * natural language corpus models
# MAGIC * image datasets like ImageNet (http://www.image-net.org/) or Google's Open Image Dataset (https://research.googleblog.com/2016/09/introducing-open-images-dataset.html)
# MAGIC * video datasets like the YouTube 8 million video dataset (https://research.googleblog.com/2016/09/announcing-youtube-8m-large-and-diverse.html)
# MAGIC 
# MAGIC [2] Augmenting a pretrained model with new training data, or using it in a related context (see Transfer Learning)
# MAGIC 
# MAGIC [3] Use a known, established network type (topology) but train on your own data
# MAGIC 
# MAGIC [4] Modify established network models for your specific problem
# MAGIC 
# MAGIC [5] Research and experiment with new types of models
# MAGIC 
# MAGIC __Just because Google DeepMind, Facebook, and Microsoft are getting press for doing a lot of new research doesn't mean you have to do it too.__
# MAGIC 
# MAGIC <img src="http://i.imgur.com/XczCfNR.png" width=500>
# MAGIC <img src="http://i.imgur.com/vcaj99I.jpg" width=500>
# MAGIC 
# MAGIC Data science and machine learning is challenging in general for enterprises (though some industries, such as pharma, have been doing it for a long time). Deep learning takes that even further, since deep learning experiments may require new kinds of hardware ... in some ways, it's more like chemistry than the average IT project!

# COMMAND ----------

# MAGIC %md ### Tools and Processes for your Deep Learning Pipeline
# MAGIC 
# MAGIC #### Data Munging
# MAGIC 
# MAGIC Most of the deep learning toolkits are focused on model-building performance or flexibility, less on production data processing.
# MAGIC 
# MAGIC However, Google recently introduced 
# MAGIC   * `tf.Transform`, a data processing pipeline project: https://github.com/tensorflow/transform 
# MAGIC   * and Dataset, an API for data processing and feeding
# MAGIC     * https://www.tensorflow.org/api_docs/python/tf/data/Dataset
# MAGIC     * only supports "flat-map" and similar operations, no shuffle/reduce despite the method called shuffle :)
# MAGIC 
# MAGIC TensorFlow can read from HDFS and run on Hadoop although it *does not* scale out automatically on a Hadoop/Spark cluster: https://www.tensorflow.org/deploy/hadoop
# MAGIC 
# MAGIC Falling back to "regular" tools, we have __Apache Spark__ for big data, and the SciPy Python family of pandas, sklearn, numpy, ...
# MAGIC 
# MAGIC #### Experimenting and Training
# MAGIC 
# MAGIC Once you want to scale beyond your laptop, there are few options...
# MAGIC 
# MAGIC * Deep-learning-infrastructure as a Service
# MAGIC     * AWS GPU-enabled instances + Deep Learning AMI (for single machine work)
# MAGIC     * "Floyd aims to be the Heroku of Deep Learning" https://www.floydhub.com/
# MAGIC     * "Effortless infrastructure for deep learning" https://www.crestle.com/
# MAGIC     * "GitHub of Machine Learning / We provide machine learning platform-as-a-service." https://valohai.com/     
# MAGIC     * "Machine Learning for Everyone" https://machinelabs.ai
# MAGIC     *  Algo hosting / model deployment + marketplace https://algorithmia.com/  
# MAGIC     * Google Cloud Platform "Cloud Machine Learning Engine" https://cloud.google.com/ml-engine/
# MAGIC     * Amazon Deep Learning AMI + CloudFormation for scale-out training https://aws.amazon.com/blogs/compute/distributed-deep-learning-made-easy/
# MAGIC 
# MAGIC * Scale Out on Your Own infrastructure or VMs
# MAGIC     * Distributed TensorFlow is free, OSS
# MAGIC     * Horovod https://github.com/uber/horovod
# MAGIC     * Apache Spark combined with Horovod (Databricks DLP), Intel BigDL (CPU) or DeepLearning4J (GPU)
# MAGIC     * TensorFlowOnSpark
# MAGIC     * CERN Dist Keras (Spark + Keras) https://github.com/cerndb/dist-keras
# MAGIC     
# MAGIC #### Frameworks
# MAGIC 
# MAGIC We've focused on TensorFlow and Keras, because that's where the "center of mass" is at the moment.
# MAGIC 
# MAGIC But there are lots of others. Major ones include:
# MAGIC * Caffe
# MAGIC * PaddlePaddle
# MAGIC * Theano
# MAGIC * CNTK
# MAGIC * MXNet
# MAGIC * DeepLearning4J
# MAGIC * BigDL
# MAGIC * Torch/PyTorch
# MAGIC * NVIDIA Digits
# MAGIC 
# MAGIC and there are at least a dozen more minor ones.
# MAGIC 
# MAGIC #### Taking Your Trained Model to Production
# MAGIC 
# MAGIC Most trained models can predict in production in near-zero time. (Recall the forward pass is just a bunch of multiplication and addition with a few other calculations thrown in.)
# MAGIC 
# MAGIC For a neat example, you can persist Keras models and load them to run live in a browser with Keras.js
# MAGIC 
# MAGIC See Keras.js for code and demos: https://github.com/transcranial/keras-js
# MAGIC 
# MAGIC <img src="http://i.imgur.com/5xx62zw.png" width=700>
# MAGIC 
# MAGIC TensorFlow Lite for fast inference on iOS or Android: https://www.tensorflow.org/mobile/tflite/
# MAGIC 
# MAGIC and Apple CoreML supports Keras models: https://developer.apple.com/documentation/coreml/converting_trained_models_to_core_ml
# MAGIC 
# MAGIC (remember, the model is already trained, we're just predicting here)
# MAGIC 
# MAGIC #### And for your server-side model update-and-serve tasks, or bulk prediction at scale...
# MAGIC 
# MAGIC (imagine classifying huge batches of images, or analyzing millions of chat messages or emails)
# MAGIC 
# MAGIC * TensorFlow has a project called TensorFlow Serving: https://tensorflow.github.io/serving/
# MAGIC * Spark Deep Learning Pipelines (bulk/SQL inference) https://github.com/databricks/spark-deep-learning
# MAGIC * Apache Spark + (DL4J | BigDL | TensorFlowOnSpark | ... )
# MAGIC 
# MAGIC * DeepLearning4J can import your Keras model: https://deeplearning4j.org/model-import-keras
# MAGIC     * (which is a really nice contribution, but not magic -- remember the model is just a pile of weights, convolution kernels, etc. ... in the worst case, many thousands of floats)
# MAGIC 
# MAGIC * MLeap http://mleap-docs.combust.ml/

# COMMAND ----------

# MAGIC %md ### Security and Robustness
# MAGIC 
# MAGIC A recent (3/2017) paper on general key failure modes is __Failures of Deep Learning__: https://arxiv.org/abs/1703.07950
# MAGIC 
# MAGIC Deep learning models are subject to a variety of unexpected perturbations and adversarial data -- even when they seem to "understand," they definitely don't understand in a way that is similar to us.
# MAGIC 
# MAGIC <img src="http://i.imgur.com/3LjF9xl.png">
# MAGIC 
# MAGIC Ian Goodfellow has distilled and referenced some of the research here: https://openai.com/blog/adversarial-example-research/
# MAGIC * He is also maintainer of an open-source project to measure robustness to adversarial examples, Clever Hans: https://github.com/tensorflow/cleverhans
# MAGIC * Another good project in that space is Foolbox: https://github.com/bethgelab/foolbox
# MAGIC 
# MAGIC ##### It's all fun and games until a few tiny stickers that a human won't even notice ... turn a stop sign into a "go" sign for your self-driving car ... __and that's exactly what this team of researchers has done__ in _Robust Physical-World Attacks on Machine Learning Models_: https://arxiv.org/pdf/1707.08945v1.pdf
# MAGIC 
# MAGIC It turns out there are a number of potential security and/or privacy vulnerabilities in common approaches to deep learning. For example, we might naively assume that once we've trained a model on millions of data points, the model weights -- which are hopefully optimized to the patterns in the data rather than individual records -- can be safely shared even if the training data itself is sensitive. In fact, this isn't necessarily true: in a number of cases, training record details can be reconstructed from the model weights or even black-box probing of the model.
# MAGIC 
# MAGIC This video by Ian Goodfellow talks about a number of these security and robustness issues: https://www.youtube.com/watch?v=Z98huoix02s
# MAGIC 
# MAGIC Articles in this curated list cover a number of security related issues for deep learning and ML in general: https://github.com/OpenMined/awesome-ai-privacy/blob/master/README.md
# MAGIC 
# MAGIC A recent post by infosec legend Bruce Schneier -- https://www.schneier.com/blog/archives/2018/03/extracting_secr.html -- briefly discusses a research paper on leakage of secrets (original paper at https://arxiv.org/pdf/1802.08232.pdf) and offers some useful links and comments.

# COMMAND ----------

# MAGIC %md # Final Notes
# MAGIC 
# MAGIC The research and projects are coming so fast that this will probably be outdated by the time you see it ...
# MAGIC 
# MAGIC ##### 2017 was the last ILSVRC! http://image-net.org/challenges/beyond_ilsvrc.php
# MAGIC 
# MAGIC ##### But there are not shortage of critical research topics for competition: https://nips.cc/Conferences/2018/CompetitionTrack
# MAGIC 
# MAGIC A few other items to explore:
# MAGIC   * Try visualizing principal components of high-dimensional data with __TensorFlow Embedding Projector__ http://projector.tensorflow.org/
# MAGIC   * Or explore with Google / PAIR's Facets tool: https://pair-code.github.io/facets/
# MAGIC   * Visualize the behavior of Keras models with keras-vis: https://raghakot.github.io/keras-vis/
# MAGIC   * Want more out of Keras without coding it yourself? See if your needs are covered in the extension repo for keras, keras-contrib: https://github.com/farizrahman4u/keras-contrib
# MAGIC   * Interested in a slightly different approach to APIs, featuring interactive (imperative) execution? In the past year, a lot of people have started using PyTorch: http://pytorch.org/
# MAGIC   * __XLA__, an experimental compiler to make TensorFlow even faster: https://www.tensorflow.org/performance/xla/
# MAGIC 
# MAGIC And in addition to refinements of what we've already talked about, there is bleeding-edge work in
# MAGIC * Neural Turing Machines
# MAGIC * Code-generating Networks
# MAGIC * Network-designing Networks
# MAGIC * Evolution Strategies (ES) as an alternative to DQL / PG: https://arxiv.org/abs/1703.03864
# MAGIC   * Here Eder Santana explores his Catch demo with ES: https://medium.com/@edersantana/mve-series-playing-catch-with-keras-and-an-evolution-strategy-a005b75d0505

# COMMAND ----------

# MAGIC %md # Books
# MAGIC 
# MAGIC To fill in gaps, refresh your memory, gain deeper intuition and understanding, and explore theoretical underpinnings of deep learning...
# MAGIC 
# MAGIC #### Easier intro books (less math)
# MAGIC 
# MAGIC *Hands-On Machine Learning with Scikit-Learn and TensorFlow: Concepts, Tools, and Techniques to Build Intelligent Systems* by Aurélien Géron 
# MAGIC 
# MAGIC *Deep Learning with Python* by Francois Chollet
# MAGIC 
# MAGIC *Fundamentals of Machine Learning for Predictive Data Analytics: Algorithms, Worked Examples, and Case Studies* by John D. Kelleher, Brian Mac Namee, Aoife D'Arcy
# MAGIC 
# MAGIC #### More thorough books (more math)
# MAGIC 
# MAGIC *Deep Learning* by Ian Goodfellow, Yoshua Bengio, Aaron Courville
# MAGIC 
# MAGIC *Information Theory, Inference and Learning Algorithms 1st Edition* by David J. C. MacKay 

# COMMAND ----------

