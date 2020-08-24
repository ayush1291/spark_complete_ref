package chap5.csv_load_example;

import com.opencsv.CSVReader;

import java.io.IOException;
import java.io.StringReader;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

// We may use wholeTextFiles here and then call reader.readAll

public class Main {
  public static void main(String[] args) throws IOException {

    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("hello");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    // Load Data
    JavaRDD<String> input = sc.textFile(args[0], 2);

    JavaRDD<Person> personJavaRDD = loadCsvData(input);
    personJavaRDD.foreach(v -> System.out.println("Output *********** " + v.getId() + " " + v.getName()));
  }

  public static JavaRDD<Person> loadCsvData(JavaRDD<String> input) {
    input.foreach(v -> System.out.println("Input ********* " + v));

    return input.map(new Function<String, Person>() {
      @Override
      public Person call(String v1) throws Exception {
        CSVReader reader = new CSVReader(new StringReader(v1));
        String[] str = reader.readNext();
        return new Person(str[0], str[1]);
      }
    });
  }
}
