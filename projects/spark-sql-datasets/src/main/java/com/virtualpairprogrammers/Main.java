package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Main {

  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "C:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();

    Dataset<Row> dataset = spark.read().option("header", true)
        .csv("src/main/resources/exams/students.csv");
    dataset.show();

    long numberOfRows = dataset.count();
    System.out.println("There are " + numberOfRows + " records");

    // take single row from datasets
    System.out.println("[Dataset Basics]");
    Row firstRow = dataset.first();
    String subject = firstRow.get(2).toString();
    String subject2 = firstRow.getAs("subject").toString();
    System.out.println(subject);
    System.out.println(subject2);

    int year = Integer.parseInt(firstRow.getAs("year"));
    System.out.println("The year was" + year);

    // filters using expression
    System.out.println("[Filters using Expression]");
    Dataset<Row> modernArtResults1 = dataset.filter("subject = 'Modern Art' AND year >= 200");
    modernArtResults1.show();

    // filters using labmda
    System.out.println("[Filters using Lambda]");
    Dataset<Row> modernArtResults2 = dataset.filter(row -> row.getAs("subject").equals("Modern Art")
        && Integer.parseInt(row.getAs("year")) > 200);
    modernArtResults2.show();

    // filters using columns
    System.out.println("[Filters using Columns]");
    Column subjectColumn = dataset.col("subject");
    Column yearColumn = dataset.col("year");
    Dataset<Row> modernArtResults3 = dataset.filter(
        subjectColumn.equalTo("Modern Art").and(yearColumn.geq(2007)));
    modernArtResults3.show();

    // filters using columns with functions
    System.out.println("[Filters using Columns with Functions]");
    Dataset<Row> modernArtResults4 = dataset.filter(
        col("subject").equalTo("Modern Art").and(col("year").geq(2007)));
    modernArtResults4.show();

    spark.close();
  }
}
