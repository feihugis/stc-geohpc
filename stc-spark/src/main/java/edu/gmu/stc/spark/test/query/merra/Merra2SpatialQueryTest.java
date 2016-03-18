package edu.gmu.stc.spark.test.query.merra;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.gmu.stc.datavisualization.netcdf.PngFactory;
import edu.gmu.stc.datavisualization.netcdf.test.NetCDFManager;
import edu.gmu.stc.hadoop.raster.DataChunk;
import edu.gmu.stc.hadoop.raster.hdf5.ArrayFloatSerializer;
import edu.gmu.stc.hadoop.raster.hdf5.ArrayIntSerializer;
import edu.gmu.stc.hadoop.raster.hdf5.H5Chunk;
import edu.gmu.stc.hadoop.raster.hdf5.H5FileInputFormat;
import edu.gmu.stc.hadoop.vector.Point;
import edu.gmu.stc.hadoop.vector.Polygon;
import edu.gmu.stc.hadoop.vector.Rectangle;
import edu.gmu.stc.hadoop.vector.extension.CountyFeature;
import edu.gmu.stc.hadoop.verctor.dataformat.geojson.CountyMultiPolygonJSON;
import edu.gmu.stc.hadoop.verctor.dataformat.geojson.CountyPolygonJSON;
import edu.gmu.stc.spark.io.kryo.SparkKryoRegistrator;
import scala.Tuple2;
import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import ucar.ma2.ArrayInt;
import ucar.ma2.Index;
import ucar.ma2.Index2D;
import ucar.ma2.Index3D;

/**
 * Created by Fei Hu on 3/14/16.
 */
public class Merra2SpatialQueryTest {

  public static void main(String[] args) throws ClassNotFoundException {

    final SparkConf sconf = new SparkConf().setAppName("SparkTest").setMaster("local[4]");

    sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sconf.set("spark.kryo.registrator", SparkKryoRegistrator.class.getName());

    JavaSparkContext sc = new JavaSparkContext(sconf);
    Configuration hconf = new Configuration();
    String vars = "LWTNET";;//"UFLXKE,AUTCNVRN,BKGERR";

    hconf.set("mapreduce.input.fileinputformat.inputdir", "/Users/feihu/Documents/Data/Merra2/");

    hconf.setStrings("variables", vars);
    //hconf.setStrings("variables", args[0]);
    hconf.setStrings("bbox", "[0-1,0-361,0-540]");
    hconf.setStrings("startTime", "19800101");
    hconf.setStrings("endTime", "20151201");
    hconf.setStrings("datanodeNum", "14");
    hconf.setStrings("slotNum", "10");
    hconf.setStrings("threadNumPerNode", "10");


    final int width = 576, height = 364;
    final double xResolution = 360.0/width; //MetaData.MERRA2.lonUnit;
    final double yResolution = -180.0/height; //MetaData.MERRA2.latUnit;
    final double x_orig = -180.0; //MetaData.MERRA2.lon_orig;
    final double y_orig = 90.0; //MetaData.MERRA2.lat_orig;
    final int[] picShape = new int[] {height,width};

    ArrayIntSerializer maskLocal = new ArrayIntSerializer(picShape, new int[picShape[0] * picShape[1]]);

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
        if (((v1.contains("\"Polygon\"") || v1.contains("\"MultiPolygon\""))&& !v1.contains("Alaska"))) {
          return true;
        } else {
          return false;
        }
      }
    });

    JavaPairRDD<String, CountyFeature> countyRDD = geoJson.mapToPair(
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
                Point point = new Point(x, y);
                for (int c=0; c<feature.getFeature().size(); c++) {
                  Polygon polygon = feature.getFeature().get(c).toLogicView(xResolution, yResolution, x_orig, y_orig);
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
          if (maskLocal.getArray().get(maskIndex) > 0) {
            continue;
          } else {
            polygonIndex.set(y-corner[0], x-corner[1]);
            maskLocal.getArray().set(maskIndex, tuple._2().getArray().get(polygonIndex));
          }
        }
      }
    }



    final Broadcast<ArrayIntSerializer> mask = sc.broadcast(maskLocal);

    PngFactory.drawPNG(mask.value().getArray(), "/Users/feihu/Desktop/test/boundary" + ".png", 0.0f, 1.0f);

    JavaPairRDD<DataChunk, ArrayFloatSerializer> records = sc.newAPIHadoopRDD(hconf,
                                                                              H5FileInputFormat.class,
                                                                              DataChunk.class,
                                                                              ArrayFloatSerializer.class);

    //the upside of y are change to downside, and filtered by the mask
    JavaPairRDD<DataChunk, ArrayFloatSerializer> recordsWithCoordinateChanging = records.mapToPair(
        new PairFunction<Tuple2<DataChunk, ArrayFloatSerializer>, DataChunk, ArrayFloatSerializer>() {
          @Override
          public Tuple2<DataChunk, ArrayFloatSerializer> call(Tuple2<DataChunk, ArrayFloatSerializer> tuple2) throws Exception {
            ArrayFloatSerializer v1 = tuple2._2();
            ArrayFloat result = (ArrayFloat) Array.factory(float.class, v1.getArray().getShape());
            Index index = Index.factory(result.getShape());
            Index rawIndex = Index.factory(result.getShape());
            Index maskIndex = Index.factory(mask.value().getArray().getShape());
            int[] corner = tuple2._1().getCorner();

            switch (index.getRank()) {
              case 3:
                int ysize = result.getShape()[1];
                for (int t=0; t<index.getShape(0); t++) {
                  for (int y=0; y<index.getShape(1); y++) {
                    for (int x=0; x<index.getShape(2); x++) {
                      index.set(t,y,x);
                      if (mask.value().getArray().get(maskIndex.set(y+corner[1],x+corner[2]))>0) {
                        if (v1.getArray().get(rawIndex.set(t, rawIndex.getShape(1)-y-1, x))>1000) {
                          result.set(index,-1.0f);
                        }
                        result.set(index,v1.getArray().get(rawIndex.set(t, /*rawIndex.getShape(1)-y-1*/y, x)));
                      } else {
                        //result.set(index,v1.getArray().get(rawIndex.set(t, /*rawIndex.getShape(1)-y-1*/y, x)));
                        result.set(index,0.0f);
                      }
                    }
                  }
                }
                break;
            }
            return new Tuple2<DataChunk, ArrayFloatSerializer>(tuple2._1(), new ArrayFloatSerializer(result.getShape(), (float[]) result.getStorage()));
          }
        });


    JavaPairRDD<String, Tuple2<DataChunk, ArrayFloatSerializer>> rddCombinedByVarAndTime = recordsWithCoordinateChanging.mapToPair(
        new PairFunction<Tuple2<DataChunk, ArrayFloatSerializer>, String, Tuple2<DataChunk, ArrayFloatSerializer>>() {
          @Override
          public Tuple2<String, Tuple2<DataChunk, ArrayFloatSerializer>> call(
              Tuple2<DataChunk, ArrayFloatSerializer> tuple2) throws Exception {
            String key = tuple2._1.getVarShortName() + tuple2._1().getCorner()[0] + "_" + tuple2._1().getFilePath().split("\\.")[2];
            return new Tuple2<String, Tuple2<DataChunk, ArrayFloatSerializer>>(key, tuple2);
          }
        });

    JavaPairRDD<String, ArrayFloatSerializer> combinedChunks = rddCombinedByVarAndTime.groupByKey().mapToPair(
        new PairFunction<Tuple2<String, Iterable<Tuple2<DataChunk, ArrayFloatSerializer>>>, String, ArrayFloatSerializer>() {
          @Override
          public Tuple2<String, ArrayFloatSerializer> call(
              Tuple2<String, Iterable<Tuple2<DataChunk, ArrayFloatSerializer>>> stringIterableTuple2)
              throws Exception {
            float[] defaultValues = new float[height*width];
            for (int i=0; i<height; i++) {
              for (int j=0; j<width; j++) {
                defaultValues[i*width+j] = 0.0f;
              }
            }
            ArrayFloat combinedArray = (ArrayFloat) Array.factory(float.class, new int[]{height, width}, defaultValues);
            Index2D maskIndex = new Index2D(new int[]{height, width});

            Iterator<Tuple2<DataChunk, ArrayFloatSerializer>> itor = stringIterableTuple2._2().iterator();
            while (itor.hasNext()) {
              Tuple2<DataChunk, ArrayFloatSerializer> tuple = itor.next();
              int[] corner = tuple._1().getCorner();
              int[] shape = tuple._1().getShape();
              Index3D chunkIndex = new Index3D(shape);
              for (int y = corner[1]; y< corner[1] + shape[1]; y++) {
                for (int x = corner[2]; x < corner[2] + shape[2]; x++) {
                  maskIndex.set(y,x);
                  combinedArray.set(maskIndex,tuple._2().getArray().get(chunkIndex.set(0, y-corner[1], x-corner[2])));
                }
              }
            }

            return new Tuple2<String, ArrayFloatSerializer>(stringIterableTuple2._1(),
                                                            new ArrayFloatSerializer(combinedArray.getShape(),
                                                                                     (float[]) combinedArray.getStorage()));
          }
        });

    combinedChunks.foreach(new VoidFunction<Tuple2<String, ArrayFloatSerializer>>() {
      @Override
      public void call(Tuple2<String, ArrayFloatSerializer> tuple) throws Exception {
        //NetCDFManager manager = new NetCDFManager();
        //ArrayFloat.D2 d2 = (ArrayFloat.D2) tuple._2().getArray();
        //ArrayFloat.D2 input = manager.interpolateArray(d2, 2, 2.0f);
        PngFactory.drawPNG(tuple._2().getArray(), "/Users/feihu/Desktop/test/"+ tuple._1() + ".png", 107.2249f, 319.2336f);
      }
    });

  }

}
