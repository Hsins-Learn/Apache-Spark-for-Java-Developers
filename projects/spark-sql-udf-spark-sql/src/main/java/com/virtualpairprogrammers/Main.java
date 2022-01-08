package com.virtualpairprogrammers;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
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

    Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

    SimpleDateFormat input = new SimpleDateFormat("MMMM", Locale.ENGLISH);
    SimpleDateFormat output = new SimpleDateFormat("M", Locale.ENGLISH);

    spark.udf().register("monthNum", (String month) -> {
      Date inputDate = input.parse(month);
      return Integer.parseInt(output.format(inputDate));
    }, DataTypes.IntegerType);

    dataset.createOrReplaceTempView("logging_table");
    Dataset<Row> results = spark.sql("SELECT level, date_format(datetime, 'MMMM') AS month, COUNT(1) AS total " +
        "FROM logging_table GROUP BY level, month ORDER BY monthNum(month), level");

    results.show();

    spark.close();
  }

}
