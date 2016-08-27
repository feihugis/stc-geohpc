package edu.gmu.stc.spark.io.etl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import edu.gmu.stc.configure.MyProperty;
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
 * Created by Fei Hu on 7/8/16.
 */
public class GeoETL {
  private static final Log LOG = LogFactory.getLog(GeoETL.class);

  public static void main(String[] args)
      throws ClassNotFoundException, IOException, InterruptedException {
    if (args.length != 8) {
      System.out.println("Please input 8 paprameters, e.g. <spark.master>, <vars>, <filepath>, <startTime>, <endTime>, <statename>, <isObject>, <isGlobal>"
                         + "for example: local[6] EVAP /Users/feihu/Documents/Data/Merra2/ 19800101 20151201 Alaska,Hawaii,Puerto false true /Users/feihu/Documents/GitHub/stc-geohpc/stc-website/src/main/resources/static/img/gif");
      return;
    }
    String jobName = "spark" + args[3] + "-" + args[4] + "-" + args[5].split(",").length + '-' + args[1].split(",").length;
    final SparkConf sconf = new SparkConf().setAppName(jobName);//.setMaster("local[6]");
    sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sconf.set("spark.kryo.registrator", SparkKryoRegistrator.class.getName());

    //sconf.set("spark.master", args[0]);
    String vars = args[1];
    String filePath = args[2];
    String startTime = args[3];
    String endTime = args[4];
    args[5] = "Arizona,California,Nevada,Texas,Utah,Colorado,New Mexico,Nebraska,Oklahoma,Kansas,Louisiana,Arkansas,Missouri,Iowa,Illinois,Mississippi,Tennessee,Kentucky,Alabama,West Virginia,Indiana,Ohio,Pennsylvania,Connecticut,Rhode Island,Massachusetts,New Jersey, Delaware,Maryland,Virginia,North Carolina,South Carolina,Georgia,Florida,Maine,New Hampshire,New York,Vermont,Michigan,Minnesota";
    final String[] stateNames = args[5].split(",");
    final boolean isObject = Boolean.parseBoolean(args[6]);
    boolean isGlobal = Boolean.parseBoolean(args[7]);
    //final String outputPath = args[8];   //    /Users/feihu/Desktop/test/

    JavaSparkContext sc = new JavaSparkContext(sconf);
    Configuration hconf = new Configuration();
    hconf.set("fs.defaultFS", MyProperty.nameNode);
    hconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    hconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    hconf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

    /*String vars = "EVAP";//"LWTNET,UFLXKE,AUTCNVRN,BKGERR";
    hconf.set("mapreduce.input.fileinputformat.inputdir", "/Users/feihu/Documents/Data/Merra2/");
    hconf.setStrings("startTime", "19800101");
    hconf.setStrings("endTime", "20151201");*/

    String geoJSONPath = MyProperty.geoJSONPath;     //"/Users/feihu/Desktop/gz_2010_us_040_00_500k.json";


    /*final String[] stateNames = new String[]{"Alaska", "Hawaii", "Puerto"}; //new String[]{"Alaska", "Hawaii", "Puerto"};
    final boolean isObject = false;
    boolean isGlobal = true;*/

    hconf.set("mapreduce.input.fileinputformat.inputdir", filePath);
    hconf.setStrings("startTime", startTime);
    hconf.setStrings("endTime", endTime);


    hconf.setStrings("bbox", "[0-24,0-361,0-576]");
    hconf.setStrings("variables", vars);

    //Did not use the following setting
    //hconf.setStrings("datanodeNum", "14");
    //hconf.setStrings("slotNum", "10");
    //hconf.setStrings("threadNumPerNode", "10");

    final int width = 576, height = 364;
    final double xResolution = 360.0 / width; //MetaData.MERRA2.lonUnit;
    final double yResolution = 180.0 / height; //MetaData.MERRA2.latUnit;
    final double x_orig = -180.0; //MetaData.MERRA2.lon_orig;
    final double y_orig = -90.0; //MetaData.MERRA2.lat_orig;
    final int interplateScale = 1;
    final int pngScale = 4;

    List<Integer> geometryIDList = new ArrayList<Integer>();

    //generate the filter mask
    Tuple2<H5Chunk, ArrayIntSerializer> maskLocal = null;
    if (isGlobal) {
      double x_min = Double.MAX_VALUE, x_max = -1 * Double.MAX_VALUE,
          y_min = Double.MAX_VALUE, y_max = -1 * Double.MAX_VALUE;
      Rectangle queryBBox = new Rectangle(x_min, y_min, x_max, y_max);
      hconf.set("geoBBox", queryBBox.toWKT());
      queryBBox = new Rectangle(-185, -95, 185, 95);
      hconf.set("geoBBox", queryBBox.toWKT());
      int[] globalmask = new int[height * width];
      for (int i = 0; i < height; i++) {
        for (int j = 0; j < width; j++) {
          globalmask[i * width + j] = 1;
        }
      }
      H5Chunk chunk = new H5Chunk();
      chunk.setShape(new int[]{height, width});
      chunk.setCorner(new int[]{0, 0});
      maskLocal = new Tuple2<H5Chunk, ArrayIntSerializer>(chunk, new ArrayIntSerializer(new int[]{height, width}, globalmask));
    } else {
      JavaRDD<String> geoJson = sc.textFile(geoJSONPath).filter(new GeoExtracting.GeoJSONFilter(stateNames, isObject));
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
        geometryIDList.addAll(MetaData.MERRA2.xyTogeometryID(rectangle.getMinX(), rectangle.getMinY(), rectangle.getMaxX(), rectangle.getMaxY()));
      }

      HashSet h = new HashSet(geometryIDList);
      geometryIDList.clear();
      geometryIDList.addAll(h);


      Rectangle queryBBox = new Rectangle(x_min, y_min, x_max, y_max);
      hconf.set("geoBBox", queryBBox.toWKT());

      JavaPairRDD<H5Chunk, ArrayIntSerializer> stateMask = states.mapToPair(new GeoExtracting.ChunkMaskFactory(xResolution, yResolution, x_orig, y_orig));
      List<Tuple2<H5Chunk, ArrayIntSerializer>> maskList = stateMask.collect();
      maskLocal = GeoExtracting.combineChunkMasks(maskList);
    }

    //PngFactory.drawPNG(maskLocal._2().getArray(), "/Users/feihu/Desktop/test/boundary" + ".png", 0.0f, 1.0f, null, pngScale);

    /*int[] bboxcorner = maskLocal._1().getCorner();
    int[] bboxshape = maskLocal._1().getShape();
    int start_geometry_id = bboxcorner[0]/MetaData.MERRA2.latChunkShape*4 + bboxcorner[1]/MetaData.MERRA2.lonChunkShape;
    int end_geometry_id = (bboxcorner[0] + bboxshape[0])/MetaData.MERRA2.latChunkShape * 4 + (bboxcorner[1] + bboxshape[1])/MetaData.MERRA2.lonChunkShape;
    int lon_size = (end_geometry_id - start_geometry_id)%4;
    int lat_size = (end_geometry_id - start_geometry_id)/4;*/
    String geometryIDs = "";
   /* for (int y=0; y<=lat_size; y++) {
      for (int x=0; x<=lon_size; x++) {
        geometryIDs = geometryIDs + (start_geometry_id + y*4 + x) + ",";
      }
    }*/

    for (Integer id: geometryIDList) {
      geometryIDs = geometryIDs + id + ",";
    }

    geometryIDs = "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15";

    hconf.set("geometryIDs", geometryIDs);


    LOG.info("***************** Broadcast start : " + System.currentTimeMillis());
    final Broadcast<Tuple2<H5Chunk, ArrayIntSerializer>> mask = sc.broadcast(maskLocal);
    LOG.info("***************** Broadcast end : " + System.currentTimeMillis());

    JavaPairRDD<DataChunk, ArrayFloatSerializer> records = sc.newAPIHadoopRDD(hconf, H5FileInputFormat.class,
                                                                              DataChunk.class,
                                                                              ArrayFloatSerializer.class);

    //filter out the data by the mask
    JavaPairRDD<DataChunk, ArrayFloatSerializer> recordsWithCoordinateChanging = records.mapToPair(
        new GeoExtracting.ChunkExtractingByMask(mask)).filter(new Function<Tuple2<DataChunk, ArrayFloatSerializer>, Boolean>() {
      @Override
      public Boolean call(Tuple2<DataChunk, ArrayFloatSerializer> v1) throws Exception {
        if (v1 == null) {
          return false;
        } else {
          return true;
        }
      }
    });

    LOG.info("***************** HDF5 I/O end : " + System.currentTimeMillis());

    JavaPairRDD<String, ArrayFloatSerializer> hourlyRDD = recordsWithCoordinateChanging.mapToPair(
        new PairFunction<Tuple2<DataChunk, ArrayFloatSerializer>, String, ArrayFloatSerializer>() {
          @Override
          public Tuple2<String, ArrayFloatSerializer> call(Tuple2<DataChunk, ArrayFloatSerializer> tuple2) throws Exception {
            //String key = tuple2._1.getVarShortName() + "_" + tuple2._1().getFilePath().split("\\.")[2].substring(0,7); // + "_" + tuple2._1().getCorner()[0];
            String key = tuple2._1.getVarShortName() + "_" + tuple2._1().getTime()/100;
            return new Tuple2<String, ArrayFloatSerializer>(key, tuple2._2());
          }
        });

    JavaPairRDD<String, Tuple2<Long, Float>> reduceMonthRDD = hourlyRDD.mapValues(
        new Function<ArrayFloatSerializer, Tuple2<Long, Float>>() {
          @Override
          public Tuple2<Long, Float> call(ArrayFloatSerializer v1) throws Exception {
            Long count = 0L;
            float sum = 0.0f;
            ArrayFloat values = v1.getArray();
            long size = values.getSize();
            for (int i =0; i<size; i++) {
              float value = values.getFloat(i);
              if (value != GeoExtracting.image_fill_value && value < MetaData.MERRA2.fillValue) {
                sum = sum + values.getFloat(i);
                count = count + 1;
              }
            }
            return new Tuple2<Long, Float>(count, sum);
          }
        }).reduceByKey(
        new Function2<Tuple2<Long, Float>, Tuple2<Long, Float>, Tuple2<Long, Float>>() {
          @Override
          public Tuple2<Long, Float> call(Tuple2<Long, Float> v1, Tuple2<Long, Float> v2)
              throws Exception {
            return new Tuple2<Long, Float>(v1._1()+v2._1(), v2._2()+v2._2());
          }
        });

    JavaPairRDD<String, Float> monthlyMeanRDD = reduceMonthRDD.mapValues(
        new Function<Tuple2<Long, Float>, Float>() {
          @Override
          public Float call(Tuple2<Long, Float> v1) throws Exception {
            return v1._2()/v1._1();
          }
        });


    //meanHourlyRDD.saveAsTextFile("/Output/Merra2/mean/"+System.currentTimeMillis());
    //meanHourlyRDD.saveAsTextFile("/Users/feihu/Desktop/Merra2/mean/"+System.currentTimeMillis());
    monthlyMeanRDD.foreach(new VoidFunction<Tuple2<String, Float>>() {
      @Override
      public void call(Tuple2<String, Float> stringFloatTuple2) throws Exception {
        System.out.println(" +++++++  mean result: " + stringFloatTuple2._1() + "   value :" + stringFloatTuple2._2());
        //LOG.info(" +++++++  mean result: " + stringFloatTuple2._1() + "   value :" + stringFloatTuple2._2());
      }
    });

  }

}
