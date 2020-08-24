package chap4.advance_partitions;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

//here, if we dont use persist method on one of the datasets, then it is shuffled again and again whenever it is used in the join method, to avoid that, we persist on some partition strategy so that spark knows beforehand where the partitions are distributed and what kind of keys are there in them, so that it could be reused when join- we can test it in cluster

public class Main {
  public static void main(String[] args) {

    System.out.println("here");

    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("hello");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    // Load Data
    JavaRDD<String> input = sc.textFile(args[0], 2);
    JavaRDD<String> input2 = sc.textFile(args[0], 5);

    JavaRDD<String> splitted = input.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String s) throws Exception {
        return Arrays.asList(s.split(" ")).iterator();
      }
    });

    JavaPairRDD<String, Integer> splitted2 = splitted.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) throws Exception {
        return new Tuple2<>(s, 1);
      }
    });

    JavaRDD<String> splitted3 = input2.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String s) throws Exception {
        return Arrays.asList(s.split(" ")).iterator();
      }
    });

    JavaPairRDD<String, Integer> splitted4 = splitted3.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) throws Exception {
        return new Tuple2<>(s, 1);
      }
    });

    JavaPairRDD splittedn = splitted2.partitionBy(new HashPartitioner(100)).persist(StorageLevel.MEMORY_ONLY_SER());
    // System.out.println((splitted2.join(splitted4)).toDebugString());
    // System.out.println(splitted2.join(splitted4).toDebugString());

    System.out.println(splittedn.toDebugString());
    splittedn.first();
    JavaPairRDD anotherPair = splittedn.join(splitted4);

    System.out.println(anotherPair.toDebugString());
    anotherPair.first();

  }
}
