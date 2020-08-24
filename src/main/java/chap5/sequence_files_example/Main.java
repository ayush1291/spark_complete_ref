package chap5.sequence_files_example;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

// We may use wholeTextFiles here and then call reader.readAll

public class Main {
  public static void main(String[] args) throws IOException {

    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("hello");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    // Load Data
    JavaRDD<String> input = sc.textFile(args[0], 2);

    JavaPairRDD<String, String> pairRDD = sc.wholeTextFiles(args[0]);
    pairRDD.mapToPair((tuple2) -> new Tuple2<Text, Text>(new Text(tuple2._1), new Text(tuple2._2))).saveAsHadoopFile(
        "/Users/ayushkumar/Downloads/sparksamples/wholtetext/seqoutput/1", Text.class, Text.class,
        SequenceFileOutputFormat.class);

  }

}
