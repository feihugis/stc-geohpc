package edu.gmu.stc.spark.test.query.merra;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.hadoop.vector.Polygon;
import edu.gmu.stc.hadoop.vector.extension.CountyFeature;
import edu.gmu.stc.hadoop.verctor.dataformat.geojson.CountyMultiPolygonJSON;
import edu.gmu.stc.hadoop.verctor.dataformat.geojson.CountyPolygonJSON;

/**
 * Created by Fei Hu on 2/21/16.
 */
public class Merra2LocalTest {

  public static void main(String[] args) {
    SparkConf sconf = new SparkConf().setAppName("SparkTest").setMaster("local[4]");
    JavaSparkContext sc = new JavaSparkContext(sconf);
    JavaRDD<String> geoJson = sc.textFile("/Users/feihu/Desktop/gz_2010_us_050_00_5m.json").filter(
        new Function<String, Boolean>() {
          @Override
          public Boolean call(String v1) throws Exception {
            if(v1.equals(",")) {
              return false;
            } else {
              return true;
            }
          }
        });

    JavaRDD<CountyFeature> polygonRDD = geoJson.flatMap(new FlatMapFunction<String, CountyFeature>() {
      @Override
      public Iterable<CountyFeature> call(String in) throws Exception {
        List<CountyFeature> countyFeatures = new ArrayList<CountyFeature>();

        Gson gson = new GsonBuilder().create();
        if (in.contains("\"Polygon\"")) {
          CountyPolygonJSON countyPlgn = gson.fromJson(in, CountyPolygonJSON.class);
          countyFeatures.add(new CountyFeature(countyPlgn.getType(), countyPlgn.getProperties().getGEO_ID(),
                                               countyPlgn.getProperties().getSTATE(),
                                               countyPlgn.getProperties().getCOUNTY(),
                                               countyPlgn.getProperties().getNAME(),
                                               countyPlgn.getProperties().getLSAD(),
                                               countyPlgn.getProperties().getCENSUSAREA(),
                                               countyPlgn.getGeometry().toPolygon())
          );
        }

        if (in.contains("\"MultiPolygon\"")) {
          CountyMultiPolygonJSON countyPlgn = gson.fromJson(in, CountyMultiPolygonJSON.class);
          countyFeatures.add(new CountyFeature(countyPlgn.getType(), countyPlgn.getProperties().getGEO_ID(),
                                               countyPlgn.getProperties().getSTATE(),
                                               countyPlgn.getProperties().getCOUNTY(),
                                               countyPlgn.getProperties().getNAME(),
                                               countyPlgn.getProperties().getLSAD(),
                                               countyPlgn.getProperties().getCENSUSAREA(),
                                               countyPlgn.getGeometry().toMultiPolygons())
          );
        }
      return countyFeatures;
      }
    });
    
  }
}
