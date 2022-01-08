# Introduction

  - [Overview](#overview)
  - [Spark Architecture](#spark-architecture)
  - [Basic Spark Appllication with Java](#basic-spark-appllication-with-java)

## Overview

**Hadoop** is a popular parallel execution framework, which uses the MapReduce framework to crunch big datasets. But it suffers from two problems:

1. It's a very rigid model. We must do a map and then a reduced process, which is powerful but maybe not suitable for all requirements.
2. If we have complex requirements, then we need to chain together multiple map and reduce processes. After one map produces finished, the results have to be written to disk and then reloaded into next map task.

**Spark** is also a parallel computing framework which enables us to perform operations on big datasets in a distributed fashion.

- It allow us to build richer jobs and we can use combinations of not just maps and reduces, but also operations such as sorts, filters and joins.
- It built something called an **execution plan**. This is where it builds a graph representing the work that we want to do and only when we're ready to get the data results that we want.

When we run Spark on a single computer, we get the full benefits of multi-threading.

<br/>
<div align="right">
  <b><a href="#introduction">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## Spark Architecture

<p align="center">
  <img src="https://i.imgur.com/kgPA3Oz.png" alt="The Repository Pattern"/>
</p>

What will happen when we deploy Spark to a cluster is the code that we've just seen will be uploaded to **Driver Node (or Master Node)**.

- The **Driver Node** will build the _execution plan_ for dividing the work into tasks that can be processed in parallel.
- The **Worker Nodes** are physical separate computers. When the execution plan is complete, the Driver Node will send the tasks to the Worker Nodes.
- What will happen is that the data from Hadoop or Amazon S3 will be distributed across the worker nodes. And it will also be partitioned where a partition is just a block of data. (A node may have multiple partitions.)

<br/>
<div align="right">
  <b><a href="#introduction">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## Basic Spark Appllication with Java

It's great that we don't have to install anything special on our local development computer. We're going to write a regular Java project and the Spark distribution is embedded in a regular JAR file.

- We're going to use Java 8 for this course. (Spark 2 is not support Java 9 yet.)
- Dependencies: `spark-core`, `spark-sql`, `hadoop-hdfs`.

Let's create a simple class named `Main.java`:

```java
package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

    public static void main(String [] args) {
        List<Double> inputData = new ArrayList<>();
        inputData.add(35.53);
        inputData.add(12.49);
        inputData.add(90.32);
        inputData.add(20.14);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Double> myRdd = sc.parallelize(inputData);
        sc.close();
    }
}
```

- The name for `SparkConf().setAppName()` will appear in reports.
- The `loca[*]` in `SparkConf().setMaster()` means that we're going to run Spark locally without a cluster.
- The `JavaSparkContext` represents a connection to our Spark cluster.
- The `parallelize()` method takes the collection of data and turns it into an RDD.
- The `JavaRDD` could be thought as a wrapper for communicating with Scala RDD. And it's a generic class so we're able to specify the type of data.

> Remember that we haven't really "loaded an RDD". We've added to the "Execution Plan" which means when we do something, the data will be loaded. At present, NO DATA HAS BEEN LOADED!

<br/>
<div align="right">
  <b><a href="#introduction">[ ↥ Back To Top ]</a></b>
</div>
<br/>