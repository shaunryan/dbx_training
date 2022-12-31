# Databricks notebook source
# MAGIC %md # TensorFlow (2018) is Google's Platform for Machine Learning
# MAGIC 
# MAGIC #### TensorFlow Library:
# MAGIC <img src="https://i.imgur.com/9xVDX1l.png" width=800>
# MAGIC 
# MAGIC #### TensorFlow (the platform) includes a model-serving component, a debugger, and other components as well

# COMMAND ----------

# MAGIC %md ## TensorFlow ("classic" / core) ... is a general math framework
# MAGIC 
# MAGIC TensorFlow is designed to accommodate...
# MAGIC 
# MAGIC * Easy operations on tensors (n-dimensional arrays)
# MAGIC * Mappings to performant low-level implementations, including native CPU and GPU
# MAGIC * Optimization via gradient descent variants
# MAGIC     * Including high-performance differentiation
# MAGIC     
# MAGIC The core contains
# MAGIC   * Low-level math primitives called "Ops"
# MAGIC   * Above these primitives, linear algebra and then other higher-level constructs (e.g., layers and common neural-net components)
# MAGIC 
# MAGIC In the past, before the "Estimators" were added, several people created high-level wrappers above the mid- and low-level areas
# MAGIC 
# MAGIC Keras, the libary we have been using, started out as one of those high-level wrappers and was eventually adopted (late 2016) as *the* official wrapper, later included in the TensorFlow distribution itself.

# COMMAND ----------

# MAGIC %md ### We'll get a little familiarity with the lower levels of TensorFlow so that it is not a "magic black box"
# MAGIC 
# MAGIC But for most of our work, it will be more productive to work with the higher-level wrappers. At the end of this notebook, we'll make the connection between the Keras API we've used and the TensorFlow code underneath.  

# COMMAND ----------

import tensorflow as tf

x = tf.constant(100, name='x')
y = tf.Variable(x + 50, name='y')

print(y)

# COMMAND ----------

# MAGIC %md ### There's a bit of "ceremony" there...
# MAGIC 
# MAGIC ... and ... where's the actual output?
# MAGIC 
# MAGIC For performance reasons, TensorFlow separates the design of the computation from the actual execution.
# MAGIC 
# MAGIC TensorFlow programs describe a computation graph -- an abstract DAG of data flow -- that can then be analyzed, optimized, and implemented on a variety of hardware, as well as potentially scheduled across a cluster of separate machines.
# MAGIC 
# MAGIC Like many query engines and compute graph engines, evaluation is __lazy__ ... so we don't get "real numbers" until we force TensorFlow to run the calculation:

# COMMAND ----------

init_node = tf.global_variables_initializer()

with tf.Session() as session:
    session.run(init_node)
    print(session.run(y))

# COMMAND ----------

# MAGIC %md ### TensorFlow integrates tightly with NumPy
# MAGIC 
# MAGIC and we typically use NumPy to create and manage the tensors (vectors, matrices, etc.) that will "flow" through our graph
# MAGIC 
# MAGIC New to NumPy? Grab a cheat sheet: https://s3.amazonaws.com/assets.datacamp.com/blog_assets/Numpy_Python_Cheat_Sheet.pdf

# COMMAND ----------

import numpy as np

data = np.random.normal(loc=10.0, scale=2.0, size=[3,3]) # mean 10, std dev 2

print(data)

# COMMAND ----------

# all nodes get added to default graph (unless we specify otherwise)
# we can reset the default graph -- so it's not cluttered up:
tf.reset_default_graph()

x = tf.constant(data, name='x')
y = tf.Variable(x * 10, name='y')

init_node = tf.global_variables_initializer()

with tf.Session() as session:
    session.run(init_node)
    print(session.run(y))

# COMMAND ----------

# MAGIC %md ### We will often iterate on a calculation ... 
# MAGIC 
# MAGIC Calling `session.run` runs just one step, so we can iterate using Python as a control:

# COMMAND ----------

with tf.Session() as session:
    for i in range(3):
        x = x + 1
        print(session.run(x))
        print("----------------------------------------------")

# COMMAND ----------

# MAGIC %md ### Optimizers
# MAGIC 
# MAGIC TF includes a set of built-in algorithm implementations (though you could certainly write them yourself) for performing optimization.
# MAGIC 
# MAGIC These are oriented around gradient-descent methods, with a set of handy extension flavors to make things converge faster.

# COMMAND ----------

# MAGIC %md #### Using TF optimizer to solve problems
# MAGIC 
# MAGIC We can use the optimizers to solve anything (not just neural networks) so let's start with a simple equation.
# MAGIC 
# MAGIC We supply a bunch of data points, that represent inputs. We will generate them based on a known, simple equation (y will always be 2\*x + 6) but we won't tell TF that. Instead, we will give TF a function structure ... linear with 2 parameters, and let TF try to figure out the parameters by minimizing an error function.
# MAGIC 
# MAGIC What is the error function? 
# MAGIC 
# MAGIC The "real" error is the absolute value of the difference between TF's current approximation and our ground-truth y value.
# MAGIC 
# MAGIC But absolute value is not a friendly function to work with there, so instead we'll square it. That gets us a nice, smooth function that TF can work with, and it's just as good:

# COMMAND ----------

x = tf.placeholder("float")
y = tf.placeholder("float")

m = tf.Variable([1.0], name="m-slope-coefficient") # initial values ... for now they don't matter much
b = tf.Variable([1.0], name="b-intercept")

y_model = tf.multiply(x, m) + b

error = tf.square(y - y_model)

train_op = tf.train.GradientDescentOptimizer(0.01).minimize(error)

model = tf.global_variables_initializer()

with tf.Session() as session:
    session.run(model)
    for i in range(5000):
        x_value = np.random.rand()
        y_value = x_value * 2 + 6 # we know these params, but we're making TF learn them
        session.run(train_op, feed_dict={x: x_value, y: y_value})

    out = session.run([m, b])
    print(out)
    print("Model: {r:.3f}x + {s:.3f}".format(r=out[0][0], s=out[1][0]))

# COMMAND ----------

# MAGIC %md #### That's pretty terrible :)
# MAGIC 
# MAGIC Try two experiments. Change the number of iterations the optimizer runs, and -- independently -- try changing the learning rate (that's the number we passed to `GradientDescentOptimizer`)
# MAGIC 
# MAGIC See what happens with different values.

# COMMAND ----------

# MAGIC %md #### These are scalars. Where do the tensors come in?
# MAGIC 
# MAGIC Using matrices allows us to represent (and, with the right hardware, compute) the data-weight dot products for lots of data vectors (a mini batch) and lots of weight vectors (neurons) at the same time. 
# MAGIC 
# MAGIC Tensors are useful because some of our data "vectors" are really multidimensional -- for example, with a color image we may want to preserve height, width, and color planes. We can hold multiple color images, with their shapes, in a 4-D (or 4 "axis") tensor.

# COMMAND ----------

# MAGIC %md ### Let's also make the connection from Keras down to Tensorflow.
# MAGIC 
# MAGIC We used a Keras class called `Dense`, which represents a "fully-connected" layer of -- in this case -- linear perceptrons. Let's look at the source code to that, just to see that there's no mystery.
# MAGIC 
# MAGIC https://github.com/fchollet/keras/blob/master/keras/layers/core.py
# MAGIC 
# MAGIC It calls down to the "back end" by calling `output = K.dot(inputs, self.kernel)` where `kernel` means this layer's weights.
# MAGIC 
# MAGIC `K` represents the pluggable backend wrapper. You can trace K.dot on Tensorflow by looking at
# MAGIC 
# MAGIC https://github.com/fchollet/keras/blob/master/keras/backend/tensorflow_backend.py
# MAGIC 
# MAGIC Look for `def dot(x, y):` and look right toward the end of the method. The math is done by calling `tf.matmul(x, y)`

# COMMAND ----------

# MAGIC %md #### What else helps Tensorflow (and other frameworks) run fast?
# MAGIC 
# MAGIC * A fast, simple mechanism for calculating all of the partial derivatives we need, called *reverse-mode autodifferentiation*
# MAGIC * Implementations of low-level operations in optimized CPU code (e.g., C++, MKL) and GPU code (CUDA/CuDNN/HLSL)
# MAGIC * Support for distributed parallel training, although parallelizing deep learning is non-trivial ... not automagic like with, e.g., Apache Spark

# COMMAND ----------

# MAGIC %md ### That is the essence of TensorFlow!
# MAGIC 
# MAGIC There are three principal directions to explore further:
# MAGIC 
# MAGIC * Working with tensors instead of scalars: this is not intellectually difficult, but takes some practice to wrangle the shaping and re-shaping of tensors. If you get the shape of a tensor wrong, your script will blow up. Just takes practice.
# MAGIC 
# MAGIC * Building more complex models. You can write these yourself using lower level "Ops" -- like matrix multiply -- or using higher level classes like `tf.layers.dense` *Use the source, Luke!*
# MAGIC 
# MAGIC * Operations and integration ecosystem: as TensorFlow has matured, it is easier to integrate additional tools and solve the peripheral problems:
# MAGIC   * Recent addition of "eager" execution mode
# MAGIC     * Easier interactive experimentation
# MAGIC     * Define-by-run semantics
# MAGIC       * Aims at a similar experience to using PyTorch
# MAGIC       * Some people agree, others think too much boilerplate in TFE; YMMV & you decide :)
# MAGIC     * https://www.tensorflow.org/programmers_guide/eager
# MAGIC   * TensorBoard for visualizing training
# MAGIC   * tfdbg command-line debugger
# MAGIC   * Distributed TensorFlow for clustered training
# MAGIC   * GPU integration
# MAGIC   * Feeding large datasets from external files
# MAGIC   * Tensorflow Serving for serving models (i.e., using an existing model to predict on new incoming data)

# COMMAND ----------

