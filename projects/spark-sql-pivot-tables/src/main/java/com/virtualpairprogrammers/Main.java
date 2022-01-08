package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Main {

  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "C:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();

    List<Row> inMemory = new ArrayList<>();
    inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
    inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
    inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
    inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
    inMemory.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));

    StructField[] fields = new StructField[]{
        new StructField("level", DataTypes.StringType, false, Metadata.empty()),
        new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
    };

    StructType schema = new StructType(fields);
    Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);

    dataset.createOrReplaceTempView("logging_table");
    Dataset<Row> results = spark.sql("SELECT level, date_format(datetime, 'MMMM') AS month FROM logging_table");
    results.show();

    spark.close();
  }
}
