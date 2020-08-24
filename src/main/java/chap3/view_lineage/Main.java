package chap3.view_lineage;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

//Check debug to string method

public class Main {
  public static void main(String[] args) {

    System.out.println("here");

    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("hello");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    // Load Data
    JavaRDD<String> input = sc.textFile(args[0]);
    System.out.println("input:");
    input.collect().forEach(v -> System.out.println(v));
    // Split into words
    JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String s) throws Exception {
        return Arrays.asList(s.split(" ")).iterator();
      }
    });

    words = words.filter(v1 -> v1.contains("hello"));

    System.out.println(words.toDebugString());

    words.collect().forEach(v -> System.out.println(v + " output"));
  }
}
