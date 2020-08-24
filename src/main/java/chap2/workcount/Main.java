package chap2.workcount;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Main {
  public static void main(String[] args) {

    System.out.println("here");

    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("hello");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    // Load Data
    JavaRDD<String> input = sc.textFile(args[0]);
    System.out.println("input:");
    input.collect().forEach(v -> System.out.println(v));
    // Split into words, we used flat map as there could be multiple lines
    JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String s) throws Exception {
        return Arrays.asList(s.split(" ")).iterator();
      }
    });
    System.out.println("Split into words");
    words.collect().forEach(v -> System.out.println(v));
    // Transform into word and count
    JavaPairRDD<String, Integer> wordsCount = words.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) throws Exception {
        return new Tuple2<>(s, 1);
      }
    }).reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer v1, Integer v2) throws Exception {
        // System.out.println("v1 and v2 " + v1 + " " + v2);
        return v1 + v2;
      }
    });
    System.out.println("Words count");
    wordsCount.collect().forEach(v -> System.out.println(v));

    // wordsCount.map(new Function<Tuple2<String, Integer>, Object>() {
    // @Override
    // public Object call(Tuple2<String, Integer> v1) throws Exception {
    // return v1._1 + " " + v1._2;
    // }
    // }).saveAsTextFile(args[1]);

    System.out.println("Output");
    wordsCount.map(new Function<Tuple2<String, Integer>, Object>() {
      @Override
      public Object call(Tuple2<String, Integer> v1) throws Exception {
        return v1._1 + " " + v1._2;
      }
    }).collect().forEach(v -> System.out.print(v + " "));
    System.out.println("OutputEnd");
  }
}
