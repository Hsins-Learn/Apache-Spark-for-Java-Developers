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
    inMemory.add(RowFactory.create("WARN", "16 December 2018"));

    StructField[] fields = new StructField[]{
        new StructField("level", DataTypes.StringType, false, Metadata.empty()),
        new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
    };

    StructType schema = new StructType(fields);
    Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);
    dataset.show();

    spark.close();
  }
}
