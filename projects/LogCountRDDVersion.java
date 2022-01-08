package com.virtualpairprogrammers;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import static com.virtualpairprogrammers.SerializableComparator.*;

public class LogCountRDDVersion 
{
	public static void main(String[] args)
	{
		System.setProperty("hadoop.home.dir", "c:/hadoop");	
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("startingSpark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> input = sc.textFile("src/main/resources/biglog.txt");
		
		// remove the csv header
		input = input.filter(line -> !line.startsWith("level,datetime"));
		
		JavaPairRDD<String, Long> pairs = input.mapToPair(rawValue -> {
			String[] csvFields = rawValue.split(",");
			String level = csvFields[0];
			String date = csvFields[1];
			String month = rawDateToMonth(date);
			String key = level + ":" + month;
			return new Tuple2<>(key, 1L);
		});
		
		JavaPairRDD<String, Long> resultsRdd = pairs.reduceByKey((value1, value2) -> value1 + value2);

		// order by
		Comparator<String> comparator = serialize( (a,b) -> {
			String monthA = a.split(":")[1];
			String monthB = b.split(":")[1];
			return monthToMonthnum(monthA) - monthToMonthnum(monthB);
		});
		
		// assuming it is a stable sort, we can sort by secondary first (level) and then sort by primary (month).
		resultsRdd = resultsRdd.sortByKey().sortByKey(comparator);
		
		List<Tuple2<String, Long>> results = resultsRdd.take(100);
		
		System.out.println("Level\tMonth\t\tTotal");
		for (Tuple2<String, Long> nextResult : results)
		{
			String[] levelMonth = nextResult._1.split(":");
			String level = levelMonth[0];
			String month = levelMonth[1];
			Long total = nextResult._2;
			System.out.println(level+"\t" + month + "\t\t" + total);
		}
	}

	private static String rawDateToMonth(String raw) {
		SimpleDateFormat rawFmt = new SimpleDateFormat("yyyy-M-d hh:mm:ss");
		SimpleDateFormat requiredFmt = new SimpleDateFormat("MMMM");
		Date results;
		try 
		{
			results = rawFmt.parse(raw);
			String month = requiredFmt.format(results);
			return month;
		} 
		catch (ParseException e) 
		{
			throw new RuntimeException(e);
		}
	}
	
	private static int monthToMonthnum(String month) {
		SimpleDateFormat rawFmt = new SimpleDateFormat("MMMM");
		SimpleDateFormat requiredFmt = new SimpleDateFormat("M");
		Date results;
		try 
		{
			results = rawFmt.parse(month);
			int monthNum = new Integer(requiredFmt.format(results));
			return monthNum;
		} 
		catch (ParseException e) 
		{
			throw new RuntimeException(e);
		}
	}
}



