package edu.gmu.stc.spark.io.etl;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.List;

import edu.gmu.stc.hadoop.vector.Rectangle;
import edu.gmu.stc.hadoop.vector.extension.CountyFeature;
import edu.gmu.stc.spark.io.kryo.SparkKryoRegistrator;
import scala.Tuple2;

/**
 * Created by Fei Hu on 9/30/16.
 */
public class GeoJSONBBox {

  public static void main(String[] args) {
    String state_5 = "Arizona,California,Nevada,Texas,Utah";
    String state_10 = "Arizona,California,Nevada,Texas,Utah,Colorado,New Mexico,Nebraska,Oklahoma,Kansas";
    String state_15 = "Arizona,California,Nevada,Texas,Utah,Colorado,New Mexico,Nebraska,Oklahoma,Kansas,Louisiana,Arkansas,Missouri,Iowa,Illinois";
    String state_20 = "Arizona,California,Nevada,Texas,Utah,Colorado,New Mexico,Nebraska,Oklahoma,Kansas,Louisiana,Arkansas,Missouri,Iowa,Illinois,Mississippi,Tennessee,Kentucky,Alabama,West Virginia";
    String state_25 = "Arizona,California,Nevada,Texas,Utah,Colorado,New Mexico,Nebraska,Oklahoma,Kansas,Louisiana,Arkansas,Missouri,Iowa,Illinois,Mississippi,Tennessee,Kentucky,Alabama,West Virginia,Indiana,Ohio,Pennsylvania,Connecticut,Rhode Island";
    String state_30 = "Arizona,California,Nevada,Texas,Utah,Colorado,New Mexico,Nebraska,Oklahoma,Kansas,Louisiana,Arkansas,Missouri,Iowa,Illinois,Mississippi,Tennessee,Kentucky,Alabama,West Virginia,Indiana,Ohio,Pennsylvania,Connecticut,Rhode Island,Massachusetts,New Jersey, Delaware,Maryland,Virginia";
    String state_35 = "Arizona,California,Nevada,Texas,Utah,Colorado,New Mexico,Nebraska,Oklahoma,Kansas,Louisiana,Arkansas,Missouri,Iowa,Illinois,Mississippi,Tennessee,Kentucky,Alabama,West Virginia,Indiana,Ohio,Pennsylvania,Connecticut,Rhode Island,Massachusetts,New Jersey, Delaware,Maryland,Virginia,North Carolina,South Carolina,Georgia,Florida,Maine";
    String state_40 = "Arizona,California,Nevada,Texas,Utah,Colorado,New Mexico,Nebraska,Oklahoma,Kansas,Louisiana,Arkansas,Missouri,Iowa,Illinois,Mississippi,Tennessee,Kentucky,Alabama,West Virginia,Indiana,Ohio,Pennsylvania,Connecticut,Rhode Island,Massachusetts,New Jersey, Delaware,Maryland,Virginia,North Carolina,South Carolina,Georgia,Florida,Maine,New Hampshire,New York,Vermont,Michigan,Minnesota";
    String state_45 = "Arizona,California,Nevada,Texas,Utah,Colorado,New Mexico,Nebraska,Oklahoma,Kansas,Louisiana,Arkansas,Missouri,Iowa,Illinois,Mississippi,Tennessee,Kentucky,Alabama,West Virginia,Indiana,Ohio,Pennsylvania,Connecticut,Rhode Island,Massachusetts,New Jersey, Delaware,Maryland,Virginia,North Carolina,South Carolina,Georgia,Florida,Maine,New Hampshire,New York,Vermont,Michigan,Minnesota,Wisconsin,North Dakota,Washington,North Dakota,Washington";
    String state_50 = "Arizona,California,Nevada,Texas,Utah,Colorado,New Mexico,Nebraska,Oklahoma,Kansas,Louisiana,Arkansas,Missouri,Iowa,Illinois,Mississippi,Tennessee,Kentucky,Alabama,West Virginia,Indiana,Ohio,Pennsylvania,Connecticut,Rhode Island,Massachusetts,New Jersey, Delaware,Maryland,Virginia,North Carolina,South Carolina,Georgia,Florida,Maine,New Hampshire,New York,Vermont,Michigan,Minnesota,Wisconsin,North Dakota,Washington,North Dakota,Washington,Idaho,Montana,Oregon,South Dakota,Wyoming";

    final SparkConf sconf = new SparkConf().setAppName("Spark GeoJSONBBox").setMaster("local[6]");
    sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sconf.set("spark.kryo.registrator", SparkKryoRegistrator.class.getName());

    final boolean isObject = true;
    String state = "Arizona,California,Nevada,Texas,Utah,Colorado,New Mexico,Nebraska,Oklahoma,Kansas,Louisiana,Arkansas,Missouri,Iowa,Illinois,Mississippi,Tennessee,Kentucky,Alabama,West Virginia,Indiana,Ohio,Pennsylvania,Connecticut,Rhode Island,Massachusetts,New Jersey, Delaware,Maryland,Virginia,North Carolina,South Carolina,Georgia,Florida,Maine,New Hampshire,New York,Vermont,Michigan,Minnesota";
    final String[] stateNames = state.split(",");
    JavaSparkContext sc = new JavaSparkContext(sconf);
    JavaRDD<String> geoJson = sc.textFile("/Users/feihu/Desktop/gz_2010_us_040_00_500k.json").filter(new GeoExtracting.GeoJSONFilter(state_50.split(","), isObject));
    JavaPairRDD<String, CountyFeature> countyRDD = geoJson.mapToPair(new GeoExtracting.GeoFeatureFactory());
    JavaRDD<CountyFeature> states = countyRDD.reduceByKey(new Function2<CountyFeature, CountyFeature, CountyFeature>() {
      @Override
      public CountyFeature call(CountyFeature v1, CountyFeature v2) throws Exception {
        CountyFeature feature = new CountyFeature(v1.getType(), v1.getGEO_ID(), v1.getSTATE(),
                                                  v1.getCOUNTY(), v1.getNAME(), v1.getLSAD(),
                                                  v1.getCENSUSAREA(),
                                                  v1.getPolygonList());
        feature.getFeature().addAll(v2.getFeature());
        return feature;
      }
    }).map(
        new Function<Tuple2<String, CountyFeature>, CountyFeature>() {
          @Override
          public CountyFeature call(Tuple2<String, CountyFeature> v1) throws Exception {
            return v1._2();
          }
        });

    JavaRDD<Rectangle> stateBoundaries = states.map(new Function<CountyFeature, Rectangle>() {
      @Override
      public Rectangle call(CountyFeature v1) throws Exception {
        return v1.getMBR();
      }
    });

    List<Rectangle> statesBList = stateBoundaries.collect();
    double x_min = Double.MAX_VALUE, x_max = -1 * Double.MAX_VALUE, y_min = Double.MAX_VALUE, y_max = -1 * Double.MAX_VALUE;
    for (Rectangle rectangle : statesBList) {
      x_min = Math.min(x_min, rectangle.getMinX());
      x_max = Math.max(x_max, rectangle.getMaxX());
      y_min = Math.min(y_min, rectangle.getMinY());
      y_max = Math.max(y_max, rectangle.getMaxY());
    }

    int lon_min = (int) (180+x_min)*8/5;
    int lon_max = (int) (180+x_max)*8/5;
    int lat_min = (int) (90+y_min)*2;
    int lat_max = (int) (90+y_max)*2;

    System.out.println(String.format("range is lat (%1$d %2$d) lon (%3$d %4$d)", lat_min, lat_max, lon_min, lon_max));

  }

}
