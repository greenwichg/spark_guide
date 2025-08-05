# If you're learning Apache Spark, this article is for you

<img src="resources/one.png" alt="one" width="500">

## Intro
At the time of this writing, Apache Spark has been released in its fourth major version, which includes many improvements and innovations.

However, I believe its core and fundamentals won’t change soon.

I have written this article to help you establish a good baseline for learning and researching Spark. It distills everything I know about this infamous engine.

> Note: This article contains illustrations with many details. I recommend reading it on a laptop or PC to get the full experience.

--- 

In 2004, Google released a paper introducing a programming paradigm called MapReduce to distribute the data processing to hundreds or thousands of machines.

In MapReduce, users have to explicitly define the Map and the Reduce functions:

<img src="resources/two.jpg" alt="two" width="500">

- Map: It takes key/value pair inputs, processes them, and outputs intermediate key/value pairs. Then, all values of the same key will be grouped and passed to the Reduce tasks.

- Reduce: It receives intermediate values from Map tasks. It then merges the intermediate values from the same key using the defined logic (e.g., Count, Sum, ...)

To ensure fault tolerance (e.g., a worker dies during the process), MapReduce relies on disk to exchange intermediate data between data tasks.

<img src="resources/three.jpg" alt="three" width="500">

Based on Google's paper, Yahoo released the open-sourced implementation of MapReduce, which soon became the go-to solution for distributed data processing. It rose and dominated, but it wouldn’t last long.

The strict Map and Reduce paradigm limits the flexibility, and the disk-based data exchange might not be suitable for use cases like machine learning or interactive queries.

UC Berkeley’s AMPLab saw a problem that needed to be solved. Although cluster computing had a lot of potential, they observed that the MapReduce implementation might not be efficient.

They created Apache Spark, a functional programming-based API to simplify multistep applications, and developed a new engine for efficient in-memory data sharing across computation steps.

--- 

## Spark RDD
Unlike MapReduce, Spark relies heavily on in-memory processing. The creator introduced the Resilient Distributed Dataset (RDD) abstraction to manage Spark’s data in memory. No matter the abstraction you use, from dataset to dataframe, they are compiled into RDDs behind the scenes.

RDD represents an immutable, partitioned collection of records that can be operated on in parallel. Data inside RDD is stored in memory for as long as possible.

## Why RDD immutable
You might wonder why Spark RDDs are immutable. Here are some of my notes:

- Concurrent Processing: Immutability keeps data consistent across multiple nodes and threads, avoiding complex synchronization and race conditions.

- Lineage and Fault Tolerance: Each transformation creates a new RDD, preserving the lineage and allowing Spark to recompute lost data reliably. Mutable RDDs would make this much harder.

- Functional Programming: RDDs follow principles that emphasize immutability, making handling failures easier and maintaining data integrity.

## Properties
Each RDD in Spark has five key properties:

<img src="resources/four.jpg" alt="four" width="500">

- List of Partitions: An RDD is divided into partitions, Spark's parallelism units. Each partition is a logical data subset and can be processed independently with different executors (more on executors later).

- Computation Function: A function determines how to compute the data for each partition.

- Dependencies: The RDD tracks its dependencies on other RDDs, which describe how it was created.

- Partitioner (Optional): For key-value RDDs, a partitioner specifies how the data is partitioned, such as using a hash partitioner.

- Preferred Locations (Optional): This property lists the preferred locations for computing each partition, such as the data block locations in the HDFS.

## Lazy
When you define the RDD, its data is unavailable or transformed immediately until an action triggers the execution. This approach allows Spark to determine the most efficient way to execute the transformations. Speaking of transformation and action:

<img src="resources/five.jpg" alt="five" width="500">

- Transformations, such as map or filter, define how the data should be transformed, but they don't execute until an action forces the computation. Because RDD is immutable, Spark creates a new RDD after applying the transformation.

- Actions are the commands that Spark runs to produce output or store data, thereby driving the actual execution of the transformations.

## Fault Tolerance
Spark RDDs achieve fault tolerance through lineage.

As mentioned, Spark keeps track of each RDD’s dependencies on other RDDs, the series of transformations that created it.

Suppose any partition of an RDD is lost due to a node failure or other issues. Spark can reconstruct the lost data by reapplying the transformations to the original dataset described by the lineage.

This approach eliminates the need to replicate data across nodes or write data to disk (like MapReduce).

---

Architecture
A Spark application consists of:

<img src="resources/six.png" alt="six" width="500">

- Driver: This JVM process manages the entire Spark application, from handling user input to distributing tasks to the executors.

- Cluster Manager: This component manages the cluster of machines running the Spark application. Spark can work with various cluster managers, including YARN, Apache Mesos, or its standalone manager.

- Executors: These processes execute tasks the driver assigns and report their status and results. Each Spark application has its own set of executors.

The Spark Driver-Executors cluster differs from the cluster hosting your Spark application. To run a Spark application, there must be a cluster of machines or processes (if you’re running Spark locally) that provides resources to Spark applications.

The cluster manager manages this cluster and the machines that can host driver and executor processes, called workers.

---

## Mode
Spark has different modes of execution, which are distinguished mainly by where the driver process is located.

- Cluster Mode: The driver process is launched on a worker node alongside the executor processes in this mode. The cluster manager handles all the processes related to the Spark application.

<img src="resources/seven.jpg" alt="seven" width="500">

- Client Mode: The driver remains on the client machine that submitted the application. This setup requires the client machine to maintain the driver process throughout the application’s execution.

<img src="resources/eight.jpg" alt="eight" width="500">

- Local mode: This mode runs the entire Spark application on a single machine, achieving parallelism through multiple threads. It’s commonly used for learning Spark or testing applications in a simpler, local environment.

<img src="resources/nine.png" alt="nine" width="500">

--- 

