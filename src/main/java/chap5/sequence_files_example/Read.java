package chap5.sequence_files_example;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Read {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("hello");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    // Load Data
    JavaPairRDD<Text, Text> input =
        sc.sequenceFile("/Users/ayushkumar/Downloads/sparksamples/wholtetext/seqoutput/1", Text.class, Text.class);
    input.foreach(v -> System.out.println("Input " + v._1 + v._2));
    input.foreach(v -> System.out.println("Input " + v._1));
  }

}
