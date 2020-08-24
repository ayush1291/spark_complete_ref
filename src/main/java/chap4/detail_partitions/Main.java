package chap4.detail_partitions;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.HadoopPartition;

//here, if we dont use persist method on one of the datasets, then it is shuffled again and again whenever it is used in the join method, to avoid that, we persist on some partition strategy so that spark knows beforehand where the partitions are distributed and what kind of keys are there in them, so that it could be reused when join- we can test it in cluster

public class Main {
  public static void main(String[] args) {

    System.out.println("here");

    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("hello");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    // Load Data
    JavaRDD<String> input = sc.textFile(args[0], 2);

    JavaRDD<String> splitted = input.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String s) throws Exception {
        return Arrays.asList(s.split(" ")).iterator();
      }
    });

    // To see the list of partitions - have a look at HadoopPartition Class and HadoopRDD.scala
    List<Partition> partitions = splitted.partitions();
    HadoopPartition partition = (HadoopPartition) partitions.get(0);
    partitions.forEach(v -> System.out.println(" ***** " + v.getClass().getCanonicalName()));
    System.out.println(" ***** " + splitted.partitioner().isPresent());

    // This is just to start and see debug at call method
    splitted.first();
  }
}
