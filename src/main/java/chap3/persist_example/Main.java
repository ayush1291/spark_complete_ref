package chap3.persist_example;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

//Check output how cached partition is fetched, and in the end its written that memory serialized replicated

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
    wordsCount.persist(StorageLevel.MEMORY_ONLY_SER());
    wordsCount.collect().forEach(v -> System.out.println(v));
    int a = 2;
    JavaPairRDD<String, Integer> wordsCount2 = wordsCount;

    System.out.println("lineage ******************  " + wordsCount2.toDebugString());

    System.out.println(wordsCount2.first());
  }
}
