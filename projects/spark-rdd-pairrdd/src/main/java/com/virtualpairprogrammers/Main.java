package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.guava.collect.Iterables;
import scala.Tuple2;

public class Main {
  public static void main(String[] args) {
    List<String> inputData = new ArrayList<>();
    inputData.add("WARN: Tuesday 4 September 0405");
    inputData.add("WARN: Tuesday 4 September 0406");
    inputData.add("ERROR: Tuesday 4 September 0408");
    inputData.add("FATAL: Wednesday 5 September 1632");
    inputData.add("ERROR: Friday 8 September 1854");
    inputData.add("WARN: Saturday 8 September 1942");

    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    System.out.println("[reduceByKey]");
    sc.parallelize(inputData)
        .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
        .reduceByKey((value1, value2) -> value1 + value2)
        .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

    System.out.println("[groupByKey]");
    sc.parallelize(inputData)
        .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
        .groupByKey()
        .foreach(tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances"));

    sc.close();
  }
}
