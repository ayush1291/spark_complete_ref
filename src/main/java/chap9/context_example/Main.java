package chap9.context_example;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

// We are loading data from a text file and applying a schema on it

public class Main {
  public static void main(String[] args) throws IOException {

    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("hello");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    SQLContext sqlContext = new SQLContext(sc);

    JavaRDD<String> input = sc.textFile("/Users/ayushkumar/Downloads/sparksamples/csv.txt");
    JavaRDD<Person> input2 = input.map(v -> new Person(v.split(",")[0], v.split(",")[1]));
    Dataset<Row> dataset = sqlContext.applySchema(input2, Person.class);
    dataset.show();
  }
}
