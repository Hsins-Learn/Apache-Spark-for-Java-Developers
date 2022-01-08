package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class Main {

  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "C:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();

    spark.udf().register("hasPassed", (String grade, String subject) -> {
      if (subject.equals("Biology")) return grade.startsWith("A");
      return grade.startsWith("A") | grade.startsWith("B") | grade.startsWith("C");
    }, DataTypes.BooleanType);

    Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
    dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade"), col("subject")));

    dataset.show();

    spark.close();
  }
}
