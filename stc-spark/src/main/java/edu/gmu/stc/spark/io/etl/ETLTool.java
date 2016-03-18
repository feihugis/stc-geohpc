package edu.gmu.stc.spark.io.etl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

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
import scala.math.Ordering;
import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import ucar.ma2.Index;
import ucar.ma2.Index2D;
import ucar.ma2.Index3D;

/**
 * Created by Fei Hu on 3/17/16.
 */
public class ETLTool {

  /**
   *
   * @param args
   * @throws ClassNotFoundException
   */
  public static void main(String[] args) throws ClassNotFoundException {

    final SparkConf sconf = new SparkConf().setAppName("SparkTest").setMaster("local[6]");

    sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sconf.set("spark.kryo.registrator", SparkKryoRegistrator.class.getName());

    JavaSparkContext sc = new JavaSparkContext(sconf);
    Configuration hconf = new Configuration();
    String vars = "LWTNET";//"UFLXKE,AUTCNVRN,BKGERR";

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
    final double xResolution = 360.0 / width; //MetaData.MERRA2.lonUnit;
    final double yResolution = -180.0 / height; //MetaData.MERRA2.latUnit;
    final double x_orig = -180.0; //MetaData.MERRA2.lon_orig;
    final double y_orig = 90.0; //MetaData.MERRA2.lat_orig;
    final int zoomScale = 6;

    final String[] stateNames = new String[]{"Alaska", "Hawaii", "Puerto"};
    final boolean isObject = false;

    JavaRDD<String> geoJson = sc.textFile("/Users/feihu/Desktop/gz_2010_us_040_00_500k.json").filter(new GeoExtracting.GeoJSONFilter(stateNames, isObject));

    JavaPairRDD<String, CountyFeature> countyRDD = geoJson.mapToPair(new GeoExtracting.GeoFeatureFactory());

    JavaRDD<CountyFeature> states = countyRDD.reduceByKey(
        new Function2<CountyFeature, CountyFeature, CountyFeature>() {
          @Override
          public CountyFeature call(CountyFeature v1, CountyFeature v2) throws Exception {
            CountyFeature feature = new CountyFeature(v1.getType(),v1.getGEO_ID(),v1.getSTATE(),
                                                      v1.getCOUNTY(),v1.getNAME(),v1.getLSAD(),v1.getCENSUSAREA(),
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

    JavaPairRDD<H5Chunk, ArrayIntSerializer> stateMask = states.mapToPair( new GeoExtracting.ChunkMaskFactory(xResolution, yResolution, x_orig, y_orig));

    List<Tuple2<H5Chunk, ArrayIntSerializer>> maskList = stateMask.collect();

    Tuple2<H5Chunk, ArrayIntSerializer> maskLocal = GeoExtracting.combineChunkMasks(maskList);

    PngFactory.drawPNG(maskLocal._2().getArray(), "/Users/feihu/Desktop/test/boundary" + ".png", 0.0f, 1.0f);

    final Broadcast<Tuple2<H5Chunk, ArrayIntSerializer>> mask = sc.broadcast(maskLocal);

    JavaPairRDD<DataChunk, ArrayFloatSerializer> records = sc.newAPIHadoopRDD(hconf, H5FileInputFormat.class,
                                                                                     DataChunk.class,
                                                                                     ArrayFloatSerializer.class);

    //the upside of y are change to downside, and filtered by the mask
    JavaPairRDD<DataChunk, ArrayFloatSerializer> recordsWithCoordinateChanging = records.mapToPair(
        new PairFunction<Tuple2<DataChunk, ArrayFloatSerializer>, DataChunk, ArrayFloatSerializer>() {
          @Override
          public Tuple2<DataChunk, ArrayFloatSerializer> call(Tuple2<DataChunk, ArrayFloatSerializer> tuple2) throws Exception {
            int[] maskCorner = mask.value()._1().getCorner();
            int[] maskShape = mask.value()._1().getShape();
            Rectangle maskBBox = new Rectangle(maskCorner[1], maskCorner[0], maskCorner[1]+maskShape[1]-1, maskCorner[0]+maskShape[0]-1);

            ArrayFloatSerializer v1 = tuple2._2();
            int[] shape = tuple2._1().getShape().clone();
            int[] corner = tuple2._1().getCorner().clone();
            Rectangle chunkBBox = new Rectangle(corner[corner.length-1], corner[corner.length-2], corner[corner.length-1]+shape[corner.length-1]-1, corner[corner.length-2]+shape[corner.length-2]-1);

            if (chunkBBox.isIntersected(maskBBox)) {
              Rectangle intectedRec = chunkBBox.getIntersection(maskBBox);
              shape[shape.length-2] = (int) (intectedRec.getHeight())+1;
              shape[shape.length-1] = (int) (intectedRec.getWidth())+1;
              corner[corner.length-2] = (int) intectedRec.getMinY();
              corner[corner.length-1] = (int) intectedRec.getMinX();

              ArrayFloat result = (ArrayFloat) Array.factory(float.class, shape);
              Index index = Index.factory(shape);
              Index rawIndex = Index.factory(tuple2._2().getArray().getShape());
              int[] rawCorner = tuple2._1().getCorner();
              Index maskIndex = Index.factory(mask.value()._2().getArray().getShape());

              switch (index.getRank()) {
                case 3:
                  for (int t=0; t<shape[0]; t++) {
                    for (int y=0; y<shape[1]; y++) {
                      for (int x=0; x<shape[2]; x++) {
                        index.set(t,y,x);
                        if (maskBBox.contains(corner[2] + x, corner[1] + y)) {
                          if (mask.value()._2.getArray().get(maskIndex.set(corner[1] + y - maskCorner[0], corner[2] + x - maskCorner[1]))>0) {
                            if (v1.getArray().get(rawIndex.set(t, rawIndex.getShape(1)-y-1, x))>1000) {
                              result.set(index,-1.0f);
                              continue;
                            }
                            result.set(index,v1.getArray().get(rawIndex.set(t, /*rawIndex.getShape(1)-y-1*/y+corner[1]-rawCorner[1], x+corner[2]-rawCorner[2])));
                          }
                        } else {
                          result.set(index,0.0f);
                        }
                      }
                    }
                  }
                  break;
              }

              tuple2._1().setCorner(corner);
              tuple2._1().setShape(shape);
              return new Tuple2<DataChunk, ArrayFloatSerializer>(tuple2._1(), new ArrayFloatSerializer(result.getShape(), (float[]) result.getStorage()));
            } else {
              return null;
            }
          }
        }).filter(new Function<Tuple2<DataChunk, ArrayFloatSerializer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<DataChunk, ArrayFloatSerializer> v1) throws Exception {
              if (v1 == null) {
                return false;
              } else {
                return true;
              }
            }
    });

    JavaPairRDD<String, Tuple2<DataChunk, ArrayFloatSerializer>> rddCombinedByVarAndTime = recordsWithCoordinateChanging.mapToPair(
        new PairFunction<Tuple2<DataChunk, ArrayFloatSerializer>, String, Tuple2<DataChunk, ArrayFloatSerializer>>() {
          @Override
          public Tuple2<String, Tuple2<DataChunk, ArrayFloatSerializer>> call(
              Tuple2<DataChunk, ArrayFloatSerializer> tuple2) throws Exception {
            String key = tuple2._1.getVarShortName() + tuple2._1().getCorner()[0] + "_" + tuple2._1().getFilePath().split("\\.")[2];
            int[] shape = tuple2._1().getShape();
            int[] corner = tuple2._1().getCorner();

            NetCDFManager manager = new NetCDFManager();

            ArrayFloat.D2 d2 = (ArrayFloat.D2) tuple2._2().getArray().reduce(0);
            ArrayFloat.D2 input = manager.interpolateArray(d2, zoomScale, 2.0f);

            shape[shape.length-2] = shape[shape.length-2]*zoomScale;
            shape[shape.length-1] = shape[shape.length-1]*zoomScale;
            corner[corner.length-2] = corner[corner.length-2]*zoomScale;
            corner[corner.length-1] = corner[corner.length-1]*zoomScale;
            tuple2._1().setCorner(corner);
            tuple2._1().setShape(shape);
            return new Tuple2<String, Tuple2<DataChunk, ArrayFloatSerializer>>(key, new Tuple2<DataChunk, ArrayFloatSerializer>(tuple2._1(),
                                                                                                                                new ArrayFloatSerializer(shape, (float[]) input.getStorage())));
          }
        });

    JavaPairRDD<String, ArrayFloatSerializer> combinedChunks = rddCombinedByVarAndTime.groupByKey().mapToPair(
        new PairFunction<Tuple2<String, Iterable<Tuple2<DataChunk, ArrayFloatSerializer>>>, String, ArrayFloatSerializer>() {
          @Override
          public Tuple2<String, ArrayFloatSerializer> call(
              Tuple2<String, Iterable<Tuple2<DataChunk, ArrayFloatSerializer>>> stringIterableTuple2)
              throws Exception {
            int height = mask.value()._1().getShape()[0]*zoomScale;
            int width = mask.value()._1().getShape()[1]*zoomScale;
            int[] cornerMask = mask.value()._1().getCorner().clone();
            cornerMask[0] = cornerMask[0]*zoomScale;
            cornerMask[1] = cornerMask[1]*zoomScale;
            float[] defaultValues = new float[height*width];
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
                  //System.out.println(y + " __ " + cornerMask[0]);
                  maskIndex.set(y-cornerMask[0], x-cornerMask[1]);
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
       // NetCDFManager manager = new NetCDFManager();
        //ArrayFloat.D2 d2 = (ArrayFloat.D2) tuple._2().getArray();
        //ArrayFloat.D2 input = manager.interpolateArray(d2, 2, 2.0f);
        PngFactory.drawPNG(tuple._2().getArray(), "/Users/feihu/Desktop/test/"+ tuple._1() + ".png", 107.2249f, 319.2336f);
      }
    });
  }

}
