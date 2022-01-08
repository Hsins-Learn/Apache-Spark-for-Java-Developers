# Spark RDD

- [RDD (Resilient Distributed Dataset)](#rdd-resilient-distributed-dataset)
- [Reduces on RDDs](#reduces-on-rdds)
- [Mapping and Outputting](#mapping-and-outputting)
  - [Mapping Operations](#mapping-operations)
  - [Outputting Results to the Console](#outputting-results-to-the-console)
  - [Counting Big Data Items](#counting-big-data-items)
  - [The `NotSerializableException` Exception](#the-notserializableexception-exception)
- [Tuples](#tuples)
  - [RDDs of Objects](#rdds-of-objects)
  - [Tuples and RDDs](#tuples-and-rdds)
- [PairRDDs](#pairrdds)
  - [Overview](#overview)
  - [Building a PairRDD](#building-a-pairrdd)
  - [Coding a ReduceByKey](#coding-a-reducebykey)
  - [Using the Fluent API](#using-the-fluent-api)
  - [Group By Key](#group-by-key)
- [FlatMaps and Filters](#flatmaps-and-filters)
  - [FlatMaps](#flatmaps)
  - [Filters](#filters)
- [Reading from Disk](#reading-from-disk)
- [Sorts and Coalesce](#sorts-and-coalesce)
  - [Coalesce](#coalesce)
  - [Why Do `sort()` Not Work with `foreach()` in Spark?](#why-do-sort-not-work-with-foreach-in-spark)
  - [`coalesce()` and `collect()`](#coalesce-and-collect)
- [Deploy to AWS EMR](#deploy-to-aws-emr)
- [Joins](#joins)
  - [Inner Joins](#inner-joins)
  - [Left Outer Joins and Optionals](#left-outer-joins-and-optionals)
  - [Right Outer Joins](#right-outer-joins)
  - [Full Joins and Cartesians](#full-joins-and-cartesians)
- [RDD Performance](#rdd-performance)
  - [Transformations and Actions](#transformations-and-actions)
  - [The DAGs and SparkUI](#the-dags-and-sparkui)
  - [Narrow and Wide Transformations](#narrow-and-wide-transformations)
  - [Shuffles](#shuffles)
  - [Dealing with Key Skews](#dealing-with-key-skews)
  - [Avoid `groupByKey` and Using map-side-reduces Instead](#avoid-groupbykey-and-using-map-side-reduces-instead)
  - [Catching and Persistence](#catching-and-persistence)

## RDD (Resilient Distributed Dataset)

The **RDD (Resilient Distributed Dataset)** is the data we're going to work with. And the data is going to be distributed across multiple partitions, which are themselves distributed across multiple nodes.

- The "resilient" means that if any of the nodes fails, then the data that was present on that node can be recovered and recreated.
- We would do some operations on RDD and we're getting a new RDD in response at each step.
- What is really happening in our Spark code is that we're building an execution plan, which is a graph of operations that are going to be executed.

The **execution plan** in a lot of documentions is called a **DAG (Directed Acyclic Graph)**. It just means a graph where there're connections between nodes on the graph. Keep in mind that:

- When our code is running, we're not really building datasets until the end.
- In fact, we're actually telling Spark to build a DAG.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## Reduces on RDDs

**Reduce** is one of the fundamental operations on an RDD. When we want to perform an operation against the RDD, which is going to transform a big datasets into a single value. For example, we can sum up all the values in an RDD by given function `function = value1 + value2`:

```java
List<Double> inputData = new ArrayList<>();
inputData.add(35.53);
inputData.add(12.49);
inputData.add(90.32);
inputData.add(20.14);

// ...

JavaRDD<Double> myRdd = sc.parallelize(inputData);
Double result = myRdd.reduce((value1, value2) -> value1 + value2);
```

What happens in a reduce operation is that:

1. This function is applied to any two values in the collection. (Reduce must be able done in any order.)
2. Each operation is applied to the result of the previous operation.
3. The same process will have been happening for other nodes in the cluster.
4. Once the reduce has been applied to all of the individual nodes, the next step is all answers are gathered together onto a single node.

> We can surely declare them with type `Double` by using `Double result = myRdd.reduce( (Double value1, Double value2) -> value1 + value2 );`. But because Java knows that this RDD is working on objects of type `Double`, it will be able to infer the type of the function.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## Mapping and Outputting

### Mapping Operations

The **Map** operation on an RDD allows us to transform the structure of an RDD from one form to another.

Imagine we have an order which has been populated with a massive set of integer values, but we want to transform this RDD into an added containing the square root of those values. All the map does is it allows us to define the so-called mapping function, which is going to be for us the square root function, e.g. `function = sqrt(value)`:

```java
List<Integer> inputData = new ArrayList<>();
inputData.add(35);
inputData.add(12);
inputData.add(90);
inputData.add(20);

// ...

JavaRDD<Integer> myRdd = sc.parallelize(inputData);
JavaRDD<Double> sqrtRdd = myRdd.map(value -> Math.sqrt(value));
```

1. The function will be sent by the driver to each partition in RDD.
2. Spark will simply apply the mapping function to every single value in the original RDD. But keep in mind that _it can't modify the existing order because RDD is always immutable (means that once it's been created, it cannot be changed)_.
3. And it will simply create a new RDD for populating the new values.

> In this case, our Map is transforming intergers into doubles, but it could be any type of input data, then resulting in any type of output data.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Outputting Results to the Console

When we move towards properly big datasets, we'll need to think about how we output results. We're going to be running on a cluster so we won't have a local terminal or anything like that.

- The common pattern is we could **write this data into a file**. By writing into a file, even if that data is enormous and the file is too big to hold inside a single Java Virtual Machine. We could also write it to a file system like HDFS and then it would be distributed across multiple nodes in a cluster.
- Another common pattern for just testing is that we can **apply to an RDD**. There is a method called `foreach()` which allows us to apply a function to every single value in the RDD.

```java
// older syntax
sqrtRdd.foreach(value -> System.out.println(value));

// modern syntax
sqrtRdd.foreach(System.out::println);
```

> The `foreach()` method sounds the same as a Map, but it's actually very different. Whatever function we pass in needs to be a `void` function without return value. So, the key difference is that `foreach()` is it isn't going to build a new RDD.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Counting Big Data Items

If we want to count some values, we can use the `count()` method:

```java
System.out.println(sqrtRdd.count());
```

There is also a common trick to apply to big datasets by using just map and reduce:

```java
JavaRDD<Long> singleIntegerRdd = sqrtRdd.map(value -> 1L);
Long count = singleIntegerRdd.reduce((value1, value2) -> value1 + value2);
System.out.println(count);
```

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### The `NotSerializableException` Exception

The `System.out::println` method in the standard Java Development Kit is not serializable, so it can't be passed into a serialisation routine. So we may got the `NotSerializableException` exception if we have a sophisticated system with multiple physical CPUs.

There is a very simple fix. All we have to do is that when we're using `foreach()` method, we need to use the `collect()` method to collect the results:

```java
sqrtRdd.collect().forEach(System.out::println);
```

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## Tuples

### RDDs of Objects

Let's say that we have the requirement for some reason to store _both these integers exactly, but we also need to store their corresponding square roots_. Keeping these two pieces of data in two separate RDDs is going to be awkward to do anything useful with them. We want to keep an integer and its corresponding square roots in the same row.

In Java, we can create a new class to store the two pieces of data:

```java
public class IntegerWithSquareRoot {
  private int originalNumber;
  private double squareRoot;

  public IntegerWithSquareRoot(int value) {
    this.originalNumber = value;
    this.squareRoot = Math.sqrt(value);
  }
}
```

Then we can create a new RDD of these objects:

```java
JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);
JavaRDD<IntegerWithSquareRoot> sqrtRdd = originalIntegers.map(value -> new IntegerWithSquareRoot(value));
```

### Tuples and RDDs

The approach in previous section works fine but it's not the general pattern. Instead, we can use **Tuple** (means a collection of values) to store the two pieces of data:

```java
JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);
JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalIntegers.map(value->new Tuple2<>(value, Math.sqrt(value)));
```

> Unfortunately, the `Tuple` class is not available in Java 8. So we need to use the `scala.Tuple` class instead.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## PairRDDs

### Overview

The **PairRDD** is going to allow us to store a so-called _key and value_. The advantage of doing this is that we'll get extra methods in a pair or they are going to allow us to do operations suah as grouping key.

> The PairRDD looks very much like a Map in Java, but _we can have multiple instances of the same key_.

Imagine that we've managed to obtain terabytes of data from a Web Server, and the requirement is counting up how many warnings/errors/fatals there were and so on.

```
WARN: Tuesday 4 September 0405
WARN: Tuesday 4 September 0406
ERROR: Tuesday 4 September 0408
FATAL: Wednesday 5 September 1632
ERROR: Friday 8 September 1854
WARN: Saturday 8 September 1942
```

By using PairRDDs, we have serveral extra methods that allow us to do very rich operations against these keys.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Building a PairRDD

```java
JavaRDD<String> originalLogMessages = sc.parallelize(inputData);
JavaPairRDD<String, String> pairRdd = originalLogMessages.mapToPair(rawValue -> {
  String[] columns = rawValue.split(":");
  String level = columns[0];
  String date = columns[1];

  return new Tuple2<String, String>(level, date);
});
```

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Coding a ReduceByKey

There's a whole set of extra methods that allow us to do operations based on the key. Let's see one of those methods called `reduceByKey()`:

```java
JavaPairRDD<String, Long> pairRdd = originalLogMessages.mapToPair(rawValue -> {
  String[] columns = rawValue.split(":");
  String level = columns[0];

  return new Tuple2<>(level, 1L);
});

JavaPairRDD<String, Long> sumsRdd = pairRdd.reduceByKey((value1, value2) -> value1 + value2);
sumsRdd.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
```

1. The keys are gathered together individually before the reduction method is applied.
2. What we're going to end up with as a result of running the method is a new PairRDD with the same keys and the values that we've calculated.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Using the Fluent API

As long as we're using Java 8 Lambdas in general, the code in Java can look very similar to the code in Scala or Python. We can very easily refactor this code to a single line of executable Java code. The reason for that is all of these methods we called on the RDD are fluent.

The **Fluent API** means giving any of these operations, the end result of the operation will return an instance of the RDD that we're working on. So once we're done something, we can hit the dot `.` and then find the next operation we need:

```java
sc.parallelize(inputData)
    .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
    .reduceByKey((value1, value2) -> value1 + value2)
    .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
```

It's a nice habit to code with the original version when working an idea up and still debugging first, then doing the clean up and refactoring.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Group By Key

```java
System.out.println("groupByKey");
sc.parallelize(inputData)
    .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
    .groupByKey()
    .foreach(tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances"));
```

> [Warning] `groupByKey()` can lead to severe performance problems. In fact, it even will often cause crashes to happen on our cluster. Avoid unless we're sure there's no better alternative.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## FlatMaps and Filters

### FlatMaps

We've been familiar with the concept of running a map transformation in Apache Spark: Given an RDD, we can run a map which will take a function apply to each element of the RDD and return a new RDD. The restriction is that there must be one output value for every single input value.

There're some circumstances that we need the a result of a single input value with multiple output values (or even no outputs). And that's where a FlatMap comes in:

```java
JavaRDD<String> sentences = sc.parallelize(inputData);
JavaRDD<String> words = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
words.collect().forEach(System.out::println);
```

1. When we call `split()` method on a string, it doesn't produce a Java collection but a raw string array. It's a very common trick that we can use a convenience method inside the class `Arrays` called `asList()` to convert the raw string array into a Java collection.
2. The API specifically demands that we should return an object of type `Iterator`. Every single collection in Java has a method called `iterator()` which will return an iterator.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Filters

With **Filters**, we can take the RDD and transform it by removing elements that don't pass a certain condition:

```java
JavaRDD<String> filteredWords = words.filter(word -> word.length() > 1);
```

The `filter()` method will iterate around every elements in the RDD and return a new RDD with only the elements that pass the filter.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## Reading from Disk

In fact, it's very simple to load an external file. Because we're reading a text file, the RDD will be of type `String`:

```java
JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
```

Almost certainly we're doing this in production on a real big dataset and a huge size file can't be held in single machine. For that reason, often we're working with the `textFile()` method with some kind of a distributed file system. The path parameter could be `s3://...`, `hdfs://...`, `file://...` or any other file system.

> It's very important to relize that the `textFile()` method doesn't load that text file into memory in this virtual machine. Otherwise, we would have an out ot memory exception immediately. Instead, what the driver do, it will tell the nodes in the cluster to load in parts or partitions.

If we're using Windows, we may got the error:

```
ERROR Shell: Failed to locate the winutils binary in the hadoop binary path
java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
```

This happening is because the `textFile()` method does support Hadoop HDFS and there's some code in the `textFile()` method that looks for the [`winutils.exe`](https://github.com/steveloughran/winutils) file. The way to getting rid of this error is to add the `hadoop.home.dir` property:

```java
System.setProperty("hadoop.home.dir", "C:/hadoop");
```

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## Sorts and Coalesce

### Coalesce

The `coalesce()` method allows us to specify how many partitions we want to end up with. If we were to do a `coalecse(1)` on the RDD, then Spark would ensure that all of the data is combined into one single partition.

```java
JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
JavaPairRDD<Long, String> sorted = switched.sortByKey(false);

sorted = sorted.coalesce(1);
sorted.collect().forEach(System.out::println);
```

> Notice that it's a wrong solution for explanating the question "why do `sort()` not work with `foreach()` in Spark?".

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Why Do `sort()` Not Work with `foreach()` in Spark?

We don't need to shuffle the data or even really know about partitions for the results of an action to be "correct". We should get correct sorting regardless of how Spark has organized the data into partitions.

**We misunderstood the contract of `foreach()`**. What's happening with `foreach()` is that the driver is sending the `foreach()` operation to each partition. The contract of `foreach()` is that the function we passed into `foreach()` is sent across to each partition in parallel, and that function will execute in parallel on each partition.

- What happens on each nodes is the `println()` would be executing on each node. In fact, we wouldn't even see any outputs on the driver.
- In local mode, our computer will have multiple cores and the default configuration of Spark is that Java will be spinning off multiple threads to execute the `println()` function.

> Keep in mind that **`foreach()` executes the lambda on each partition in parallel**.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### `coalesce()` and `collect()`

After performing many transformations (and maybe actions) on our multi-Terabyte, multi-partition RDD, we've now reached the point where we only have a small amount of data. For the remaining transformations, there's no point in continuing across 1000 partitions (any shuffles will be pointlessly expensive).

- The `coalecse()` method is just a way of reducing the number of partitions, it's never needed just to give Spark a hint about how many partitions we want to end up with. **It's used for performance reasons and not for correctness reasons**.
- The `collect()` method is generally used when we've finished and we want to gather a small RDD onto the driver node for printing or processing. But remember that only call if we're sure that the RDD is small enough to fit into a single JVM's RAM.
- If the results are still "big", we'd write to a file.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## Deploy to AWS EMR

Spark supports the Hadoop cluster manager called YARN. The **EMR (Elastic MapReduce)** is basically Amazon's implementation of Hadoop in the cloud. To packing Spark JAR for EMR, we need to do following:

1. Remove `.setMaster("local[*]")` from the `SparkConf` object.
2. Change the file location from `src/main/resources/subtitles/input.txt` to `s3n://<BUCKET_NAME>/input.txt`.
3. Check carefully the `<mainClass>` in `pom.xml` file.

After package the `JAR` file, we can deploy it to EMR. Then we can run the application on EMR by following command:

```bash
$ spark-submit spark.jar
```

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## Joins

### Inner Joins

We can use the `join()` method on the RDD, it's a similar way to how we work with relational databases. The simplest option is so-called **inner join**. What happens with the `join()` method is that Spark will combine the data from both RDDs into a single RDD:

```java
JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd = visits.join(users);
```
 
<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Left Outer Joins and Optionals

When we do a **left outer join**, we start from the left RDD and then we look for the matching key in the right RDD. If we find a match, we'll return the matching key-value pair from the left RDD. If we don't find a match, we'll return a `null` value. In fact, what will we get is an object called `Optional.empty` which is a special object that represents the absence of a value.

```java
JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinedRdd = visits.leftOuterJoin(users);
joinedRdd.collect().forEach(System.out::println);

// print only the username
joinedRdd.collect().forEach(it -> System.out.println(it._2._2.orElse("blank").toUpperCase()));
```

> The idea of `Optional` is the wrappers for objects and methods that can return `null` values.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Right Outer Joins

```java
JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joinedRdd = visits.rightOuterJoin(users);
joinedRdd.collect().forEach(System.out::println);
joinedRdd.collect().forEach(it -> System.out.println(" user " + it._2._2 + " had " + it._2._1.orElse(0)));
```

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Full Joins and Cartesians

```java
JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> joinedRdd = visits.cartesian(users);
joinedRdd.collect().forEach(System.out::println);
```

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## RDD Performance

### Transformations and Actions

At runtime, we're not building our RDDs on each of these steps. In fact, what we're building is an **execution plan (or DAG)**. Only when we get to the operations where Spark actually executes the plan, is the RDD actually built for calculating the results.

There are two types of operations that Spark will execute:

- **Transformations** are the operations that will be executed on the RDD. These operations are usually **pure**, meaning that they don't change the data.
- **Actions** are going to make calculations happen, and they're generally going to result in some regular Java objects.

It's difficult to remember which operations are transformations and which are actions, but we can use the Spark documentation for checking.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### The DAGs and SparkUI

Rather than just looking at the program and trying to guess at what's happening, it's recommended to get used to looking at whe Web User Interface for getting a full view of the execution plan.

When our program is running, Spark will start a Web Server at port `4040`. We can access it and it will show us the progress of the current executing job:

- In the DAG, each of these boxes represents a transformation.
- If we click on any of these boxes, it will take us to an expanded view and very usefully will point us back to the line of code that resulted in that transformation.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Narrow and Wide Transformations

<div align="center">
  <img src="https://i.imgur.com/OzqiDNR.png" />
</div>

- Spark determine the Partitions depending on the input source for the RDD.
- Spark can implement the transformation without moving any data around. It's called **Narrow Transformation**. It didn't have to change the partitioning of the data.
- The **Wide Transformation** is the transformation that moves data around. It can change the partitioning of the data and going to be an expensive operation.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Shuffles

The **Shuffle** is the process of moving data around to make sure that the data is distributed evenly. It's triggered by any of the wide transformations and it's always an expensive operation.

- If we need to do a shuffle, we need to do a shuffle. But we can think carelly about **when** we do a wide transformation in the job.
- It's not always obvious that we've done a wide transformation too early.
- A stage is a series of transformations that don't need a shuffle. When we get to the point where a shuffle is required, then Spark creates a new stage.
- The description of stage is the last transformation that was executed in the stage. Moreover, we can see the information about Duration, Input Size, Output Size and Shuffle Size.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Dealing with Key Skews

1. If we can possibly rework out scripts so that the wide transformations happen late, then we can avoid the shuffle.
2. If that's not possible, then we might have to resort to a shuffle by adding salt to the keys.

> "Salting" just mangles the keys to artificially spread them across partitions. It means we'll have to at some point do the work to group them back together. It's a last resort, but it works.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Avoid `groupByKey` and Using map-side-reduces Instead

When we're doing `groupByKey()`, we know the shuffles will be required. We should avoid shuffles as much as possible because they're expensive.

The `reduceByKey()` is a transformation that does a map-side-reduce. It will do a reduce on each partition first. It will greatly reduce the amount of data shuffle around.

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Catching and Persistence

<br/>
<div align="right">
  <b><a href="#spark-rdd">[ ↥ Back To Top ]</a></b>
</div>
<br/>