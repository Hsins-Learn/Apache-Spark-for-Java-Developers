package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
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

    JavaRDD<String> sentences = sc.parallelize(inputData);
    JavaRDD<String> words = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
    JavaRDD<String> filteredWords = words.filter(word -> word.length() > 1);

    System.out.println("[WORDS]");
    words.collect().forEach(System.out::println);

    System.out.println("[FILTERED-WORDS]");
    filteredWords.collect().forEach(System.out::println);

    sc.close();
  }
}
