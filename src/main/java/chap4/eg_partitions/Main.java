package chap4.eg_partitions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
  public static void main(String[] args) {

    System.out.println("here");

    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("hello");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    // Load Data
    JavaRDD<String> input = sc.textFile(args[0]);

    // Try to uncomment this - this works only when partitions are reduced
    // input = input.coalesce(10);

    input = input.repartition(10);
    System.out.println("************ Number of partitions are " + input.getNumPartitions());

    JavaRDD<String> filtered = input.filter(v -> v.contains("1"));
    System.out.println("************ " + filtered.toDebugString());

    System.out.println("************ Number of partitions are " + filtered.getNumPartitions());

    JavaRDD<String> filteredMap = filtered.map(v -> v + "");
    System.out.println("*************** " + filteredMap.toDebugString());

    // Similarly if we are grouping data using pair rdd, we can pass the partitions we want the data to be grouped

  }
}
