package chap5.jdbc_load_example;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.JdbcRDD;

// We should use count(*) before calling the sql just to pass the ranges, so as to distribute the data

public class Main {
  public static void main(String[] args) throws IOException {

    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("hello");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    // Load Data
    JavaRDD<Integer> jdbcRDD = JdbcRDD.create(sc, new JdbcRDD.ConnectionFactory() {
      @Override
      public Connection getConnection() throws Exception {
        return DriverManager.getConnection("jdbc:oracle:thin:@192.168.130.51:1521:mobdb", "bd_team2", "bd_team2");
      }
    }, "select countryid from IRDB_COUNTRY_NAME where countryid>=? and countryid<=?", 300l, 310l, 5, resultSetIntegerFunction);

    jdbcRDD.foreach(v -> System.out.println("Input " + v));
  }

  public static Function<ResultSet, Integer> resultSetIntegerFunction = (ResultSet v) -> {
    try {
      return v.getInt(1);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  };
}
