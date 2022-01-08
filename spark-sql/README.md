# Spark SQL

- [Overview](#overview)
- [Datasets](#datasets)
  - [Getting Started](#getting-started)
  - [Datasets Basics](#datasets-basics)
  - [Filters using Expressions](#filters-using-expressions)
  - [Filters using Lambdas](#filters-using-lambdas)
  - [Filters using Columns](#filters-using-columns)
- [Full SQL Syntax with TemplateView](#full-sql-syntax-with-templateview)
- [In Memory Data](#in-memory-data)
- [Groupings and Aggregations](#groupings-and-aggregations)
- [Date Formatting](#date-formatting)
- [Multiple Groupings](#multiple-groupings)
- [Ordering](#ordering)
- [DataFrame API](#dataframe-api)
  - [SQL vs DataFrame](#sql-vs-dataframe)
  - [DataFrame Grouping](#dataframe-grouping)
- [Pivot Tables](#pivot-tables)
- [UDFs (User Defined Functions)](#udfs-user-defined-functions)
  - [Use Lambda to Write UDFs in Spark](#use-lambda-to-write-udfs-in-spark)
  - [More Input Parameters](#more-input-parameters)
  - [Using UDF in Spark SQL](#using-udf-in-spark-sql)
- [Performance: Spark SQL vs RDDs](#performance-spark-sql-vs-rdds)

## Overview

**Spark SQL** is designed to make it easier to work with structured data.

- Spark SQL doesn't mean that we're going to be working with databases.
- We can use Spark SQL to work with databases. But Spark SQL works with any kind of data formats.
- Spark SQL gives a very rich API for working with structured data.
- We can interact with data using the syntax very similar to SQL.

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## Datasets

### Getting Started

```java
public class Main {

  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "C:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();

    Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
    dataset.show();

    spark.close();
  }
}
```

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Datasets Basics

The `Dataset` object is the core object in Spark SQL.

```java
// take single row from datasets
Row firstRow = dataset.first();

String subject = firstRow.get(2).toString();              // [Option 1]
String subject = firstRow.getAs("subject").toString();    // [Option 2]
System.out.println(subject);

int year = Integer.parseInt(firstRow.getAs("year"));
System.out.println("The year was" + year);
```

- The `show()` method will show just a fraction of the data so we can peek at it.
- The `get()` method returns just an `Object`.
- If we have headers, we can use the `getAs()` method to get the value of specific column. It will attempt to perform an automatic conversion.

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Filters using Expressions

We're going to do operations on our datasets like aggregations, groupings and filtering. Let's say that we want to filter this datasets so that we end up with a datasets where the results are for "Modern Art" subjects:

```java
Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 200");
modernArtResults.show();
```

> The important thing here is that it doesn't read entire dataset into memory. What we're doing here is an RDD is being built up and an execution plan is being built up. **Only the point where we come to an OPERATION, and Spark will execute that plan!**

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Filters using Lambdas

Alternatively, we can use lambdas to filter the dataset:

```java
Dataset<Row> modernArtResults = dataset.filter(row -> row.getAs("subject").equals("Modern Art")
                                                   && Integer.parseInt(row.getAs("year")) > 200);
modernArtResults.show();
```

Those approaches are functionally identical. There's going to be no dramatic performance difference between the two.

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Filters using Columns

```java
Column subjectColumn = dataset.col("subject");
Column yearColumn = dataset.col("year");

Dataset<Row> modernArtResults3 = dataset.filter(subjectColumn.equalTo("Modern Art")
                                        .and(yearColumn.geq(2007)));
modernArtResults3.show();
```

Moreover, we can use following instead by using `import static org.apache.spark.sql.functions.*`:

```java
Dataset<Row> modernArtResults4 = dataset.filter(col("subject").equalTo("Modern Art")
                                        .and(col("year").geq(2007)));
modernArtResults4.show();
```

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## Full SQL Syntax with TemplateView

Let's talk about the concept of a **Temporary View** and the view we can think of as an in-memory structure, which is created from datasets. Using the temporary view, we can perform or execute full SQL syntax operations against that view.

```java
dataset.createOrReplaceTempView("my_students_table");
Dataset<Row> results = spark.sql("SELECT DISTINCT(year) FROM my_students_table ORDER BY year DESC");
results.show();
```

- Doing it doesn't return anything and it doesn't give us a new dataset.
- We can do SQL operations on the view. 

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## In Memory Data

When we're working with a query, it might be useful to have a set of hardcoded in-memory data. In this section, this would be exactly how we would make up a **unit test** if we wanted to use something like JUnit.

```java
List<Row> inMemory = new ArrayList<>();
inMemory.add(RowFactory.create("WARN", "16 December 2018"));

StructField[] fields = new StructField[] {
    new StructField("level", DataTypes.StringType, false, Metadata.empty()),
    new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
};

StructType schema = new StructType(fields);
Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);
dataset.show();
```

- The `RowFactory.create()` method take a variable number of parameters, actually as many as we want and can be any type of objects.
- All a `DataFrame` is a dataset of rows.
- The `schema` is just an object that tells Spark what the data types of each of the columns are.

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## Groupings and Aggregations

To find how many warning/fatal/info messages that we have in the log, we can use **Grouping**. In SQL, there's a `GROUP BY` clause that we can use to group the data. We also need to specify an aggregation function to perform on the grouped data since we just want to know how many rows there are.

```java
dataset.createOrReplaceTempView("logging_table");
Dataset<Row> results = spark.sql("SELECT level, COUNT(datetime) FROM logging_table GROUP BY level ORDER BY level");
results.show();
```

When we do a `GROUP BY` in SQL, what's happening is we're getting this grouping of data in the form of some kind of a collection. But the aggregation function `collect_list()` that will give us exactly such a collection.

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## Date Formatting

Sometimes, we would like to convert raw date strings into a more readable format. For example, we might want to convert the date string "2015-4-21 19:23:20" into only its month "April". This could be done with the `date_format()` function:

```java
dataset.createOrReplaceTempView("logging_table");
Dataset<Row> results = spark.sql("SELECT level, date_format(datetime, 'MMMM') AS month FROM logging_table");
results.show();
```

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## Multiple Groupings

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## Ordering

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## DataFrame API

### SQL vs DataFrame

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### DataFrame Grouping

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## Pivot Tables

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## UDFs (User Defined Functions)

### Use Lambda to Write UDFs in Spark

Let's say that we want to add columns to our dataset that are calculated from some of the other columns. We can always call the `withColumn()` method on a dataset to add a new column (remember that the dataset is immutable):

```java
Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
dataset = dataset.withColumn("pass", lit(col("grade").equalTo("A+")));
```

Sometimes we want to get any kind of complex logic, and this is where a **User Define Function (UDF)** comes into play. A UDF is a function allows us to add our own functions effectively into the Spark API:

```java
spark.udf().register("hasPassed", (String grade) -> grade.equals("A+"), DataTypes.BooleanType);

Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade")));
```

- The `spark.udf()` method itself doesn't do anything, but it's an intermediate object that allows us to call the method called `register()`.

> We can't do this using just Standard Java and have to follow some very precise steps in the Spark API to register the function.

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### More Input Parameters

The modern way to write UDFs is to use a lambda function:

```java
spark.udf().register("hasPassed", (String grade, String subject) -> {
  if (subject.equals("Biology")) return grade.startsWith("A");
  return grade.startsWith("A") | grade.startsWith("B") | grade.startsWith("C");
}, DataTypes.BooleanType);

Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade"), col("subject")));
```

Besides, we can also define the UDF with old-style function:

```java
private static UDF2<String, String, Boolean> hasPassedFunction = new UDF2<String, String, Boolean> () {
  @Override
  public Boolean call(String grade, String subject) throws Exception {
    if (subject.equals("Biology")) return grade.startWith("A");
    return grade.startsWith("A") | grade.startsWith("B") | grade.startsWith("C");
  }
};
```

- We need to create an instance of the class called `UDF2` that takes two parameters. The number is just the number of input parameters that we want to accept.

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>

### Using UDF in Spark SQL

```java
SimpleDateFormat input = new SimpleDateFormat("MMMM", Locale.ENGLISH);
SimpleDateFormat output = new SimpleDateFormat("M", Locale.ENGLISH);

spark.udf().register("monthNum", (String month) -> {
  Date inputDate = input.parse(month);
  return Integer.parseInt(output.format(inputDate));
}, DataTypes.IntegerType);

dataset.createOrReplaceTempView("logging_table");
Dataset<Row> results = spark.sql("SELECT level, date_format(datetime, 'MMMM') AS month, COUNT(1) AS total " +
                                 "FROM logging_table GROUP BY level, month ORDER BY monthNum(month), level");
```

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>

## Performance: Spark SQL vs RDDs

<br/>
<div align="right">
  <b><a href="#spark-sql">[ ↥ Back To Top ]</a></b>
</div>
<br/>