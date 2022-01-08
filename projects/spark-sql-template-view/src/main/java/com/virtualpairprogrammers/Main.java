package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "C:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();

    Dataset<Row> dataset = spark.read().option("header", true)
        .csv("src/main/resources/exams/students.csv");

    dataset.createOrReplaceTempView("my_students_table");
    Dataset<Row> results = spark.sql("SELECT DISTINCT(year) FROM my_students_table ORDER BY year DESC");
    results.show();

    spark.close();
  }
}
