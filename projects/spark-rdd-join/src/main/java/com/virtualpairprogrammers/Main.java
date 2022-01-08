package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

public class Main {
  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "C:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
    visitsRaw.add(new Tuple2<>(4, 18));
    visitsRaw.add(new Tuple2<>(6, 4));
    visitsRaw.add(new Tuple2<>(10, 9));

    List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
    usersRaw.add(new Tuple2<>(1, "John"));
    usersRaw.add(new Tuple2<>(2, "Bob"));
    usersRaw.add(new Tuple2<>(3, "Alan"));
    usersRaw.add(new Tuple2<>(4, "Doris"));
    usersRaw.add(new Tuple2<>(5, "Marybelle"));
    usersRaw.add(new Tuple2<>(6, "Raquel"));

    JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
    JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

    System.out.println("[Inner Join]");
    JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd = visits.join(users);
    joinedRdd.collect().forEach(System.out::println);

    System.out.println("[Left Outer Join]");
    JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinedRdd2 = visits.leftOuterJoin(users);
    joinedRdd2.collect().forEach(System.out::println);
    joinedRdd2.collect().forEach(it -> System.out.println(it._2._2.orElse("blank").toUpperCase()));

    System.out.println("[Right Outer Join]");
    JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joinedRdd3 = visits.rightOuterJoin(users);
    joinedRdd3.collect().forEach(System.out::println);
    joinedRdd3.collect().forEach(it -> System.out.println(" user " + it._2._2 + " had " + it._2._1.orElse(0)));

    System.out.println("[Full Joins]");
    JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> joinedRdd4 = visits.cartesian(users);
    joinedRdd4.collect().forEach(System.out::println);

    sc.close();
  }
}