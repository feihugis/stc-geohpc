package edu.gmu.stc.spark.test.query.merra;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.hadoop.vector.Polygon;
import edu.gmu.stc.hadoop.vector.extension.CountyFeature;
import edu.gmu.stc.hadoop.verctor.dataformat.geojson.CountyMultiPolygonJSON;
import edu.gmu.stc.hadoop.verctor.dataformat.geojson.CountyPolygonJSON;
import scala.Tuple2;

/**
 * Created by Fei Hu on 2/21/16.
 */
public class Merra2VectorLocalTest {

  /**
   *
   * @param args
   */
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

    /*JavaPairRDD<String,CountyFeature> stateKeyRDD = geoJson.mapToPair(
        new PairFunction<String, String, CountyFeature>() {
          @Override
          public Tuple2<String, CountyFeature> call(String s) throws Exception {
            return null;
          }
        });*/
    JavaPairRDD<String,CountyFeature> stateKeyRDD = geoJson.flatMapToPair(
        new PairFlatMapFunction<String, String, CountyFeature>() {
          @Override
          public Iterable<Tuple2<String, CountyFeature>> call(String in) throws Exception {
            List<Tuple2<String, CountyFeature>> polygons = new ArrayList<Tuple2<String, CountyFeature>>();
            Gson gson = new GsonBuilder().create();
            if (in.contains("\"Polygon\"")) {
              CountyPolygonJSON countyPlgn = gson.fromJson(in, CountyPolygonJSON.class);
              CountyFeature feature = new CountyFeature(countyPlgn.getType(), countyPlgn.getProperties().getGEO_ID(),
                                                   countyPlgn.getProperties().getSTATE(),
                                                   countyPlgn.getProperties().getCOUNTY(),
                                                   countyPlgn.getProperties().getNAME(),
                                                   countyPlgn.getProperties().getLSAD(),
                                                   countyPlgn.getProperties().getCENSUSAREA(),
                                                   countyPlgn.getGeometry().toPolygon());
              polygons.add(new Tuple2<String, CountyFeature>(feature.getSTATE(),feature));
              return polygons;
            }

            if (in.contains("\"MultiPolygon\"")) {
              CountyMultiPolygonJSON countyPlgn = gson.fromJson(in, CountyMultiPolygonJSON.class);
              CountyFeature feature = new CountyFeature(countyPlgn.getType(), countyPlgn.getProperties().getGEO_ID(),
                                                   countyPlgn.getProperties().getSTATE(),
                                                   countyPlgn.getProperties().getCOUNTY(),
                                                   countyPlgn.getProperties().getNAME(),
                                                   countyPlgn.getProperties().getLSAD(),
                                                   countyPlgn.getProperties().getCENSUSAREA(),
                                                   countyPlgn.getGeometry().toMultiPolygons());
              polygons.add(new Tuple2<String, CountyFeature>(feature.getSTATE(),feature));
              return polygons;
            }
           return null;
          }
        });

    //Combine county boundary to state boundary
    JavaPairRDD<String, CountyFeature> statesPlgnRDD = stateKeyRDD.reduceByKey(
        new Function2<CountyFeature, CountyFeature, CountyFeature>() {
          @Override
          public CountyFeature call(CountyFeature v1, CountyFeature v2) throws Exception {
            //v1.getFeature().addAll(v2.getFeature());
            return v1;
          }
        });
    JavaRDD<CountyFeature> states = statesPlgnRDD.map(
        new Function<Tuple2<String, CountyFeature>, CountyFeature>() {
          @Override
          public CountyFeature call(Tuple2<String, CountyFeature> v1) throws Exception {
            return v1._2();
          }
        });

    states.foreach(new VoidFunction<CountyFeature>() {
      @Override
      public void call(CountyFeature countyFeature) throws Exception {
        System.out.println("test");
      }
    });

    /*statesPlgnRDD.foreach(new VoidFunction<Tuple2<String, CountyFeature>>() {
      @Override
      public void call(Tuple2<String, CountyFeature> tuple2) throws Exception {
        System.out.println(tuple2._1() + " has " *//*+ tuple2._2*//* + " counties +++++++++++++++++++++");
      }
    });*/

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

    polygonRDD.foreach(new VoidFunction<CountyFeature>() {
      @Override
      public void call(CountyFeature countyFeature) throws Exception {
        System.out.println(countyFeature.getFeature().get(0).toPostGISPGgeometry().toString());
      }
    });




  }
}
