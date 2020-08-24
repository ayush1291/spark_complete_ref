package chap3.eg1;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

//Understanding of flatmap here
//We can take an rdd as a list of records and then similar to streams

public class Main {
  public static void main(String[] args) {

    System.out.println("here");

    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("hello");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    // Load Data
    JavaRDD<String> input = sc.textFile(args[0]);
    input.collect().forEach(v -> System.out.println(v + " input"));
    // Split into words
    JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String s) throws Exception {
        return Arrays.asList(new String[] { s }).iterator();
      }
    });
    JavaRDD<String[]> words2 = words.map(v -> v.split(" "));

    words.collect().forEach(v -> System.out.println(v + " output"));
    words2.collect().forEach(v -> System.out.println(v + " output2" + v[0]));
  }
}
