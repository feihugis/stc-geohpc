package edu.gmu.stc.spark.test.query.merra;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.List;

import edu.gmu.stc.datavisualization.netcdf.PngFactory;
import edu.gmu.stc.hadoop.raster.io.datastructure.ArrayIntSerializer;
import edu.gmu.stc.hadoop.raster.hdf5.H5Chunk;
import edu.gmu.stc.hadoop.vector.Point;
import edu.gmu.stc.hadoop.vector.Polygon;
import edu.gmu.stc.hadoop.vector.Rectangle;
import edu.gmu.stc.hadoop.vector.extension.CountyFeature;
import edu.gmu.stc.hadoop.vector.geojson.CountyMultiPolygonJSON;
import edu.gmu.stc.hadoop.vector.geojson.CountyPolygonJSON;
import scala.Tuple2;
import ucar.ma2.Index2D;

/**
 * Created by Fei Hu on 2/21/16.
 */
public class Merra2VectorLocalTest {

  public static void main(String[] args) throws ClassNotFoundException {
    final SparkConf sconf = new SparkConf().setAppName("SparkTest").setMaster("local[4]").registerKryoClasses(new Class<?>[]{
        Class.forName("org.apache.hadoop.io.Writable"),
        Class.forName("edu.gmu.stc.hadoop.vector.Polygon")
    });
    JavaSparkContext sc = new JavaSparkContext(sconf);
    final int width = 1000, height =500;
    final double xResolution = 400.0/width; //MetaData.MERRA2.lonUnit;
    final double yResolution = -200.0/height; //MetaData.MERRA2.latUnit;
    final double x_orig = -200.0; //MetaData.MERRA2.lon_orig;
    final double y_orig = 100.0; //MetaData.MERRA2.lat_orig;
    final int[] picShape = new int[] {height,width};

    ArrayIntSerializer mask = new ArrayIntSerializer(picShape, new int[picShape[0]*picShape[1]]);

    JavaRDD<String> geoJson = sc.textFile("/Users/feihu/Desktop/gz_2010_us_040_00_500k.json").filter(
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

    geoJson = geoJson.filter(new Function<String, Boolean>() {
      @Override
      public Boolean call(String v1) throws Exception {
        if ((v1.contains("\"Polygon\"") || v1.contains("\"MultiPolygon\""))) {
          return true;
        } else {
          return false;
        }
      }
    });

    JavaPairRDD<String,CountyFeature> countyRDD = geoJson.mapToPair(
        new PairFunction<String, String, CountyFeature>() {
          @Override
          public Tuple2<String, CountyFeature> call(String in) throws Exception {

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
              return new Tuple2<String, CountyFeature>(feature.getSTATE(), feature);
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
              return new Tuple2<String, CountyFeature>(feature.getSTATE(), feature);
            }
          return null;
          }
        });

    //Combine county boundary to state boundary
    JavaPairRDD<String, CountyFeature> statesPlgnRDD = countyRDD.reduceByKey(
        new Function2<CountyFeature, CountyFeature, CountyFeature>() {
          @Override
          public CountyFeature call(CountyFeature v1, CountyFeature v2) throws Exception {
            CountyFeature feature = new CountyFeature(v1.getType(),v1.getGEO_ID(),v1.getSTATE(),
                                                      v1.getCOUNTY(),v1.getNAME(),v1.getLSAD(),v1.getCENSUSAREA(),
                                                      v1.getPolygonList());
            feature.getFeature().addAll(v2.getFeature());
            return feature;
          }
        });

    JavaRDD<CountyFeature> states = statesPlgnRDD.map(
        new Function<Tuple2<String, CountyFeature>, CountyFeature>() {
          @Override
          public CountyFeature call(Tuple2<String, CountyFeature> v1) throws Exception {
            return v1._2();
          }
        });

    JavaPairRDD<H5Chunk, ArrayIntSerializer> stateMask = states.mapToPair(
        new PairFunction<CountyFeature, H5Chunk, ArrayIntSerializer>() {
          @Override
          public Tuple2<H5Chunk, ArrayIntSerializer> call(CountyFeature feature) throws Exception {
            Rectangle bbox = feature.getMBR().toLogicView(xResolution, yResolution, x_orig, y_orig);
            int[] corners = new int[]{(int) bbox.getMinY(), (int) bbox.getMinX()};
            int[] shape = new int[]{((int) bbox.getMaxY() - (int) bbox.getMinY() + 1), ((int) bbox.getMaxX() - (int) bbox.getMinX() + 1)};
            H5Chunk stateInfo = new H5Chunk();
            stateInfo.setCorner(corners);
            stateInfo.setShape(shape);
            int[] array = new int[shape[0] * shape[1]];
            for (int i=0; i<shape[0]; i++) {
              for (int j=0; j<shape[1]; j++) {
                array[i*shape[1] + j] = 0;
              }
            }

            for (int c=0; c<feature.getFeature().size(); c++) {
              Polygon polygon = feature.getFeature().get(c).toLogicView(xResolution, yResolution, x_orig, y_orig);
              for (int i = 0; i < polygon.getNpoints(); i++) {
                int py = (int) polygon.getYpoints()[i];
                int px = (int) polygon.getXpoints()[i];
                array[(py - corners[0]) * shape[1] + (px - corners[1])] = 1;
              }
            }

           for (double y = ((int) bbox.getMinY())+0.0; y < bbox.getMaxY(); y++) {
              for(double x = ((int) bbox.getMinX())+0.0; x < bbox.getMaxX(); x++) {
                Point point = new Point(x,y);
                for (int c=0; c<feature.getFeature().size(); c++) {
                    Polygon polygon = feature.getFeature().get(c).toLogicView(xResolution, yResolution, x_orig, y_orig);
                    Rectangle rectangle = polygon.getMBR();
                    if (polygon.contains(point.x, point.y)) {
                      array[(((int) y) - corners[0])*shape[1] + (((int) x) - corners[1])] = 1;
                    } else {
                      //array[(((int) y) - corners[0])*shape[1] + (((int) x) - corners[1])] = 0;
                    }
                }
              }
            }

            return new Tuple2<H5Chunk, ArrayIntSerializer>(stateInfo, new ArrayIntSerializer(shape, array));
          }
        });

    List<Tuple2<H5Chunk, ArrayIntSerializer>> maskList = stateMask.collect();
    Index2D maskIndex = new Index2D(new int[]{height, width});
    for (Tuple2<H5Chunk, ArrayIntSerializer> tuple : maskList) {
      int[] corner = tuple._1().getCorner();
      int[] shape = tuple._1().getShape();
      Index2D polygonIndex = new Index2D(new int[]{shape[0], shape[1]});
      for (int y = corner[0]; y< corner[0] + shape[0]; y++) {
        for (int x = corner[1]; x < corner[1] + shape[1]; x++) {

          maskIndex.set(y,x);
          if (mask.getArray().get(maskIndex) > 0) {
            continue;
          } else {
            polygonIndex.set(y-corner[0], x-corner[1]);
            mask.getArray().set(maskIndex, tuple._2().getArray().get(polygonIndex));
          }
         }
       }
      }

    PngFactory.drawPNG(mask.getArray(),"/Users/feihu/Desktop/test.png", 0.0f, 1.0f, null, 1);







  }
}
