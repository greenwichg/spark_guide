# Apache Spark Runtime Architecture

## Introduction
Apache Spark is a distributed computing platform. However, every Spark application is a distributed application in itself. I will explain that in a minute, but remember, every Spark application is a distributed application in itself.

## Understanding Distributed Applications and Clusters
A distributed application runs on a cluster, so you need a cluster for your Spark application. You can run it on your local machine for development and unit testing, but ultimately your Spark application runs on a production cluster.

### Cluster Technologies

We have two most commonly used cluster technologies for Spark:
- Hadoop YARN cluster
- Kubernetes cluster

We have a few more such as Mesos and Spark Standalone cluster, but these two cover more than 90% market share. Make sense?

## What is a Cluster?

Now let's start with the following question: What is a cluster?

A cluster is a pool of physical computers. For example, I may have a cluster of 10 machines. Each machine in this example cluster comes with 16 CPU cores and 64 GB RAM. They are networked, and we created a cluster of all these ten machines using the Hadoop YARN cluster manager. The entire pool is termed as a cluster, and individual machines are known as worker nodes. So I have a cluster of 10 worker nodes.

### Cluster Capacity Calculation

What is the total capacity of my cluster? I have ten workers, each with 16 CPU cores and 64 GB RAM. So my total CPU capacity is 160 CPU cores, and the RAM capacity is 640 GB. Make sense?

## Running a Spark Application

Now I want to run a Spark application on this cluster. So I will use the spark-submit command and submit my Spark application to the cluster. My request will go to the YARN resource manager. The YARN RM will create one Application Master container on a worker node and start my application's main() method in the container.

### Understanding Containers

But what is the container? A container is an isolated virtual runtime environment. It comes with some CPU and memory allocation. For example, let's assume YARN RM gave 4 CPU cores and 16 GB memory to this container and started it on a worker node. The worker node has got 16 CPU cores and 64 GB of memory, but YARN RM took 4 CPU cores and 16 GB memory and gave it to my container. Now my application's main() method will run in the container, and it can use 4 CPU cores and 16 GB memory.

## Inside the Container

Great! Now let's go inside the container and see what happens there. The container is running the main() method of my application, right? And we have two possibilities here:
- PySpark Application
- Scala Application

My main method could be a PySpark application, or it could be a Scala application, right? Spark comes in two commonly used flavors.

### PySpark Application Architecture

So let's assume my application is a PySpark application. But Spark is written in Scala, and it runs in the Java virtual machine. What does it mean? Let me explain.

Spark was written in Scala. Scala is a JVM language, and it always runs in the JVM. But the Spark developers wanted to bring this to Python developers. So they created a Java wrapper on top of the Scala code, and then they created a Python wrapper on top of the Java wrappers. And this Python wrapper is known as PySpark. Make sense?

### How PySpark Works

Great! So I have Python code in my main() method. This Python code is designed to start a Java main() method internally. So my PySpark application will start a JVM application. Once we have a JVM application, the PySpark wrapper will call the Java wrapper using the Py4J connection.

#### What is Py4J?

What is Py4J? Py4J allows a Python application to call a Java application. And that's how PySpark works. It will always start a JVM application and call Spark APIs in the JVM. The actual Spark application is always a Scala application running in the JVM, but PySpark is calling Java wrapper using Py4J, and the Java wrapper runs Scala code in the JVM. Make sense?

## Driver Components

Great! What do we call these two things? The PySpark main method is my PySpark Driver, and the JVM application here is my Application Driver. These two terms are critical to remember. So your Spark application driver is the main method of your application. If you wrote a PySpark application, you would have a PySpark driver and an application driver. But if you wrote it in Scala, you won't have a PySpark driver, but you will always have an application driver. Make sense?

## Distributed Processing with Executors

Great! In the lecture earlier, I told you that your Spark application is a distributed application in itself, right? So what did I mean? Let me explain.

### How Work is Distributed

Your application driver distributes the work to others. So the driver does not perform any data processing work. Instead, it will create some executors and get the work done from them. But how does it happen?

### Creating Executors

After starting, the driver will go back to the YARN RM and ask for some more containers. The RM will create some more containers on worker nodes and give them to the driver. So let's assume we got four new containers, and let's assume each container comes with 4 CPU cores and 16 GB of memory. Now the driver will start Spark executors in these containers. Each container will run one Spark executor, and the Spark executor is a JVM application.

### Driver and Executor Relationship

So your driver is a JVM application, and your executor is also a JVM application. These executors are responsible for doing all the data processing work. The driver will assign work to the executors, monitor them, and manage the overall application, but the executors do all the data processing. Make sense?

## Terminology Review

Great! So let's quickly revise some terminologies. You have a container that runs the driver. This container is also known as Application Master or AM Container. The AM container runs a Spark driver. If you submitted the PySpark code, you would have a PySpark driver, and you will also have a JVM driver. These two will communicate using Py4J. If you started a Scala or a Java application, you would have a JVM driver only.

### Container Types and Resource Usage

The driver will start first, and then it will request the Cluster RM for more containers. On receiving new containers, the driver will start executors in these new containers. We call them executor containers. The AM container, as well as the executor containers, will run on the worker nodes. Your worker node may have a physical CPU and memory, but your driver and executor can use the CPU and memory given to the container. They cannot use extra CPU or memory from the workers. Make sense?

## Runtime Architecture Variations

Great! One last thing. If you are using Spark DataFrame API in Scala or Java, your runtime architecture looks like this: You will have one JVM driver and one or more JVM executors.

### PySpark DataFrame APIs

If you are using PySpark DataFrame APIs, your runtime architecture looks like this: You will have one PySpark driver, one JVM driver, and one or more JVM executors.

### PySpark with Custom Python Code

But if you are also using some additional Python libraries that are not part of the PySpark, then your runtime architecture looks like this. Even if you are creating UDFs in Python, your runtime architecture will look like this. So what is the difference? You have a Python worker at each executor.

## Python Workers Explained

What is a Python Worker, and why do we need them? Python worker is a Python runtime environment, and you need them only if you are using some Python-specific code or libraries. PySpark is a wrapper on Java code, so as long as you are using only PySpark, you do not need a Python runtime environment. All the PySpark code is translated into Java code, and it runs in the JVM.

### When Python Workers are Needed

But if you are using some Python libraries which don't have a Java wrapper, you will need a Python runtime environment to run them. So the executors will create a Python runtime environment so they can execute your Python code. I will talk more about this in a later video, but for now, let's remember that you may have Python workers inside the executor container for running custom Python code outside the PySpark API. Make sense?

## Conclusion

Great! That's all for Spark Cluster. See you again! Keep learning and keep growing!
