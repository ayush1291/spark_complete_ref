package chap5.csv_load_example;

import com.opencsv.CSVReader;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

// We may use wholeTextFiles here and then call reader.readAll
// If there are only a few files, then we may want to repartition our input, to allow spark to effectively parallelize future operations

public class WholeTextFiles {
  public static void main(String[] args) throws IOException {

    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("hello");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    // Load Data

    JavaRDD<Person> personJavaRDD = loadCsvData(sc.wholeTextFiles(args[0]));
    personJavaRDD.foreach(v -> System.out.println("Output *********** " + v.getId() + " " + v.getName()));
  }

  public static JavaRDD<Person> loadCsvData(JavaPairRDD<String, String> input) {
    input.foreach(v -> System.out.println("Input ********* " + v));

    return input.flatMap(new FlatMapFunction<Tuple2<String, String>, Person>() {
      @Override
      public Iterator<Person> call(Tuple2<String, String> v1) throws Exception {
        CSVReader reader = new CSVReader(new StringReader(v1._2));
        return reader.readAll().stream().map(v -> new Person(v[0], v[1])).collect(Collectors.toList()).iterator();
      }
    });
  }
}
