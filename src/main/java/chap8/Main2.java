package chap8;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

import scala.collection.Iterator;

//Hops thereshold
//bintime start and end
//Input path

public class Main2 {

  public static void main(String[] args) throws AnalysisException {
    if (null == null)
      System.out.println("true");

    String input = args[0];

    SparkConf sparkConf = new SparkConf();
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    StructField[] structFields = new StructField[] { new StructField("imsi", DataTypes.LongType, true, Metadata.empty()),
        new StructField("networktripid", DataTypes.LongType, true, Metadata.empty()),
        new StructField("partnernetworkid", DataTypes.IntegerType, true, Metadata.empty()),
        new StructField("hops", DataTypes.LongType, true, Metadata.empty()) };
    SQLContext sqlContext = new SQLContext(sc);
    Dataset<Row> dataset = sqlContext.load(input, "orc");
    dataset.createTempView("map");

    dataset = sqlContext.sql(
        "select cast(imsi as bigint),steeringcos,networktripid,partnernetworkid,partnernetworkpreference, eventtime from map");

    Iterator<StructField> iterable = dataset.schema().iterator();
    StringBuilder builder = new StringBuilder();
    while (iterable.hasNext()) {
      StructField next = iterable.next();
      builder.append(next.name() + ":" + next.dataType().typeName().toLowerCase() + ":");
    }
    System.out.println(builder.toString());
  }
}
