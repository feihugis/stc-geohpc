package edu.gmu.stc.spark.io.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.List;

import edu.gmu.stc.hadoop.raster.DataChunk;
import edu.gmu.stc.hadoop.raster.io.datastructure.ArrayFloatSerializer;
import edu.gmu.stc.hadoop.raster.io.datastructure.ArrayIntSerializer;
import edu.gmu.stc.hadoop.raster.hdf5.H5Chunk;
import edu.gmu.stc.hadoop.raster.hdf5.H5FileInputFormat;
import edu.gmu.stc.hadoop.raster.index.MetaData;
import edu.gmu.stc.hadoop.vector.Rectangle;
import edu.gmu.stc.hadoop.vector.extension.CountyFeature;
import edu.gmu.stc.spark.io.kryo.SparkKryoRegistrator;
import scala.Tuple2;
import ucar.ma2.ArrayFloat;

/**
 * Created by Fei Hu on 3/17/16.
 */
public class ETLMeanLocalTest {

  /**
   *
   * @param args
   * @throws ClassNotFoundException
   */
  public static void main(final String[] args) throws ClassNotFoundException {

    if (args.length != 9) {
      System.out.println("Please input 9 input parameters, <vars> <inputdir> <bbox> <startTime> <endTime> <jsonPath> <resultPath> <states> <isQueryorExcludedstates>");
      return;
    }

    final SparkConf sconf = new SparkConf().setAppName("SparkTest").setMaster("local[6]");
    
    sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sconf.set("spark.kryo.registrator", SparkKryoRegistrator.class.getName());

    JavaSparkContext sc = new JavaSparkContext(sconf);
    Configuration hconf = new Configuration();

    hconf.setStrings("variables", args[0]); //"LWTNET,UFLXKE";//"UFLXKE,AUTCNVRN,BKGERR";
    hconf.set("mapreduce.input.fileinputformat.inputdir", args[1]);
    hconf.setStrings("bbox", args[2]); //"[0-1,0-361,0-576],[5-6,0-361,0-576]"
    hconf.setStrings("startTime", args[3]);  //"19800101"
    hconf.setStrings("endTime", args[4]); //"20151201"
    String jsonPath = args[5];
    final String resultPath = args[6];
    final String[] stateNames = args[7].split(","); //new String[]{"Alaska", "Hawaii", "Puerto"}; //new String[]{"Alaska", "Hawaii", "Puerto"};
    final boolean isObject = Boolean.parseBoolean(args[8]); //false;

    //hconf.set("mapreduce.input.fileinputformat.inputdir", "/Users/feihu/Documents/Data/Merra2/");
    hconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    hconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    hconf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

    //Did not use the following setting
    /*hconf.setStrings("datanodeNum", "14");
    hconf.setStrings("slotNum", "10");
    hconf.setStrings("threadNumPerNode", "10");*/

    final int width = 576, height = 364;
    final double xResolution = 360.0 / width; //MetaData.MERRA2.lonUnit;
    final double yResolution = 180.0 / height; //MetaData.MERRA2.latUnit;
    final double x_orig = -180.0; //MetaData.MERRA2.lon_orig;
    final double y_orig = -90.0; //MetaData.MERRA2.lat_orig;
    final int interplateScale = 1;
    final int pngScale = 4;



    JavaRDD<String> geoJson = sc.textFile(jsonPath).filter(new GeoExtracting.GeoJSONFilter(stateNames, isObject));
    //JavaRDD<String> geoJson = sc.textFile("/Users/feihu/Desktop/gz_2010_us_040_00_500k.json").filter(new GeoExtracting.GeoJSONFilter(stateNames, isObject));

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

    JavaRDD<Rectangle> stateBoundaries = states.map(new Function<CountyFeature, Rectangle>() {
      @Override
      public Rectangle call(CountyFeature v1) throws Exception {
        return v1.getMBR();
      }
    });

    List<Rectangle> statesBList = stateBoundaries.collect();
    double x_min = Double.MAX_VALUE, x_max = -1*Double.MAX_VALUE, y_min = Double.MAX_VALUE, y_max = -1*Double.MAX_VALUE;
    for (Rectangle rectangle : statesBList) {
      x_min = Math.min(x_min, rectangle.getMinX());
      x_max = Math.max(x_max, rectangle.getMaxX());
      y_min = Math.min(y_min, rectangle.getMinY());
      y_max = Math.max(y_max, rectangle.getMaxY());
    }

    Rectangle queryBBox = new Rectangle(x_min,y_min,x_max,y_max);
    hconf.set("geoBBox", queryBBox.toWKT());

    JavaPairRDD<H5Chunk, ArrayIntSerializer> stateMask = states.mapToPair( new GeoExtracting.ChunkMaskFactory(xResolution, yResolution, x_orig, y_orig));

    List<Tuple2<H5Chunk, ArrayIntSerializer>> maskList = stateMask.collect();

    Tuple2<H5Chunk, ArrayIntSerializer> maskLocal = GeoExtracting.combineChunkMasks(maskList);

    /*queryBBox = new Rectangle(-185,-95,185,95);
    hconf.set("geoBBox", queryBBox.toWKT());
    int[] globalmask = new int[height*width];
    for (int i= 0; i<height; i++) {
      for (int j=0; j<width; j++) {
        globalmask[i*width+j] = 1;
      }
    }
    maskLocal._1().setShape(new int[]{height, width});
    maskLocal._1().setCorner(new int[]{0, 0});
    maskLocal = new Tuple2<H5Chunk, ArrayIntSerializer>(maskLocal._1(), new ArrayIntSerializer(new int[]{height, width}, globalmask));*/

    //PngFactory.drawPNG(maskLocal._2().getArray(), "/Users/feihu/Desktop/test/boundary" + ".png", 0.0f, 1.0f, null, pngScale);
    //PngFactory.drawPNG(maskLocal._2().getArray(), gifPath + "/boundary" + ".png", 0.0f, 1.0f, null, pngScale);

    final Broadcast<Tuple2<H5Chunk, ArrayIntSerializer>> mask = sc.broadcast(maskLocal);

    JavaPairRDD<DataChunk, ArrayFloatSerializer> records = sc.newAPIHadoopRDD(hconf, H5FileInputFormat.class,
                                                                                     DataChunk.class,
                                                                                     ArrayFloatSerializer.class);

    //filter out the data by the mask
    JavaPairRDD<DataChunk, ArrayFloatSerializer> recordsWithCoordinateChanging = records.mapToPair(new GeoExtracting.ChunkExtractingByMask(mask))
        .filter(new Function<Tuple2<DataChunk, ArrayFloatSerializer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<DataChunk, ArrayFloatSerializer> v1) throws Exception {
              if (v1 == null) {
                return false;
              } else {
                return true;
              }
            }
    });

    JavaPairRDD<String, Tuple2<DataChunk, ArrayFloatSerializer>> rddCombinedByVarAndTime =
        recordsWithCoordinateChanging.mapToPair(new PairFunction<Tuple2<DataChunk, ArrayFloatSerializer>, String, Tuple2<DataChunk, ArrayFloatSerializer>>() {
              @Override
              public Tuple2<String, Tuple2<DataChunk, ArrayFloatSerializer>> call(
                  Tuple2<DataChunk, ArrayFloatSerializer> tuple2) throws Exception {
                String key = tuple2._1.getVarShortName() + "_" + tuple2._1().getFilePath().split("\\.")[2] + "_" + tuple2._1().getCorner()[0];
                return new Tuple2<String, Tuple2<DataChunk, ArrayFloatSerializer>>(key, tuple2);
              }
            });

    JavaPairRDD<String, ArrayFloatSerializer> combinedChunks = rddCombinedByVarAndTime.groupByKey().mapToPair( new GeoExtracting.CombineChunks(mask, interplateScale));

    JavaPairRDD<String, Float> meanRDDS = combinedChunks.mapValues(
        new Function<ArrayFloatSerializer, Float>() {
          @Override
          public Float call(ArrayFloatSerializer v1) throws Exception {
            ArrayFloat array = v1.getArray();
            float sum = 0.0f;
            int count = 0;
            for (int i=0; i<array.getSize(); i++) {
              float value = array.getFloat(i);
              if (value != MetaData.MERRA2.fillValue && value != -1.0f) {
                sum = sum + value;
                count++;
              }
            }
            return sum/count;
          }
        });

    meanRDDS.saveAsTextFile(resultPath);


  }

}
