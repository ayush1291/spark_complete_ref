package chap5.json_read_write;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.codehaus.jackson.map.ObjectMapper;

// Here we have created the object mapper inside the map method as that function is shipped from driver, so it is supposed to be serializable- and as it is not, so we constructed it at runtime inside the map function

public class Main {
  public static void main(String[] args) throws IOException {

    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("hello");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    // Load Data
    JavaRDD<String> input = sc.textFile(args[0], 2);

    JavaRDD<Person> personJavaRDD = loadJsonData(input);
    personJavaRDD.foreach(v -> System.out.println("Output *********** " + v.getId() + " " + v.getName()));
  }

  public static JavaRDD<Person> loadJsonData(JavaRDD<String> input) {
    input.foreach(v -> System.out.println("Input ********* " + v));

    return input.map(new Function<String, Person>() {
      @Override
      public Person call(String v1) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(v1, Person.class);
      }
    });
  }
}
