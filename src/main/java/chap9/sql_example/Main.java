package chap9.sql_example;

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
    dataset.registerTempTable("person");

    JavaRDD<String> input3 = sc.textFile("/Users/ayushkumar/Downloads/sparksamples/csv.txt");
    JavaRDD<Person> input4 = input3.map(v -> new Person(v.split(",")[0], v.split(",")[1]));
    Dataset<Row> dataset2 = sqlContext.applySchema(input4, Person.class);
    dataset2.registerTempTable("person2");

    // This ,method will cache the dataset and it can be seen in the ui - only till the time our driver is alive
    // dataset.cache();

    sqlContext.sql("select * from person union select * from person2").show();
    sqlContext.cacheTable("person2");
    sqlContext.sql("select * from person union select * from person2").show();

    // this will not run as it will require hive enabled
    // sqlContext.sql("create table abc");

  }
}
