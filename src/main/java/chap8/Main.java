package chap8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

//Hops thereshold
//bintime start and end
//Input path

public class Main {

  public static void main(String[] args) throws AnalysisException {
    Long binstart = Long.parseLong(args[0]);
    Long binend = Long.parseLong(args[1]);
    Long hops = Long.parseLong(args[2]);
    String input = args[3];
    String output = args[4];

    SparkConf sparkConf = new SparkConf();
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    sc.hadoopConfiguration().set("mapred.output.compress", "false");

    StructField[] structFields = new StructField[] { new StructField("imsi", DataTypes.LongType, true, Metadata.empty()),
        new StructField("networktripid", DataTypes.LongType, true, Metadata.empty()),
        new StructField("partnernetworkid", DataTypes.IntegerType, true, Metadata.empty()),
        new StructField("hops", DataTypes.LongType, true, Metadata.empty()) };
    SQLContext sqlContext = new SQLContext(sc);
    Dataset<Row> dataset = sqlContext.load(input, "orc");
    dataset.createTempView("map");

    dataset = sqlContext.sql(
        "select cast(imsi as bigint),steeringcos,networktripid,partnernetworkid,partnernetworkpreference, eventtime from map where eventtype in (2,5) and networktripid is not null and cast(bintime as bigint)>= "
            + binstart + " and cast(bintime as bigint)<=" + binend);

    KeyValueGroupedDataset<String,
        Row> groupKeySet =
            dataset.groupByKey(
                (MapFunction<Row,
                    String>) v1 -> String
                        .valueOf("" + v1.getAs("networktripid") + v1.getAs("partnernetworkid") + v1.getAs("imsi")),
                Encoders.STRING());

    Dataset<Row> finalOutput = groupKeySet.mapGroupsWithState(new MapGroupsWithStateFunction<String, Row, String, Row>() {

      @Override
      public Row call(String key, Iterator<Row> values, GroupState<String> state) throws Exception {
        Long count = -1L;
        List<MapModel> mapValues = new ArrayList<>();
        while (values.hasNext()) {
          mapValues.add(makeMapFromRow(values.next()));
        }
        Collections.sort(mapValues);
        String prev = "";
        for (MapModel mapModel : mapValues) {
          if (mapModel.getPartnernetworkpreference() == null && mapModel.getSteeringcos() == null)
            continue;
          if (prev.equalsIgnoreCase("" + mapModel.getPartnernetworkpreference() + mapModel.getSteeringcos()))
            continue;
          prev = "" + mapModel.getPartnernetworkpreference() + mapModel.getSteeringcos();
          count++;
        }
        if (count >= hops)
          return RowFactory.create(new Object[] { mapValues.get(0).getImsi(), mapValues.get(0).getNetworkTripId(),
              mapValues.get(0).getPartnernetworkid(), count });
        else
          return RowFactory.create(new Object[4]);
      }
    }, Encoders.STRING(), RowEncoder.apply(new StructType(structFields)), GroupStateTimeout.ProcessingTimeTimeout());
    DataFrameWriter<Row> writer = finalOutput.filter((FilterFunction<Row>) v -> v.get(0) != null).write();
    writer.option("header", "false").format("com.databricks.spark.csv").mode("append").save(output);
  }

  public static MapModel makeMapFromRow(Row row) {
    return new MapModel(row.getAs("imsi"), row.getAs("networktripid"), row.getAs("partnernetworkid"),
        row.getAs("partnernetworkpreference"), row.getAs("steeringcos"), row.getAs("eventtime"));
  }
}
