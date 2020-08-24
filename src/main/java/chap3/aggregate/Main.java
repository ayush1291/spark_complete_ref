package chap3.aggregate;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Main {
  public static void main(String[] args) {

    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("hello");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    // Load Data
    JavaRDD<String> input = sc.textFile(args[0]);
    System.out.println("input:");
    input.collect().forEach(v -> System.out.println(v));
    input = input.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String s) throws Exception {
        return Arrays.asList(s.split(" ")).iterator();
      }
    });

    JavaPairRDD<Integer, Integer> inputPair = input.mapToPair(new PairFunction<String, Integer, Integer>() {
      @Override
      public Tuple2<Integer, Integer> call(String s) throws Exception {
        return new Tuple2<>(Integer.parseInt(s), 1);
      }
    });

    Tuple2<Integer, Integer> tuple2 =
        inputPair.reduce(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
          @Override
          public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
            return new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2);
          }
        });

    System.out.println("Sum is " + tuple2._1);
    System.out.println("Count is " + tuple2._2);
  }
}
