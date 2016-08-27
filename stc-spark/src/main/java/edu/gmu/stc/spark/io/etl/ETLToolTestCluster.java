package edu.gmu.stc.spark.io.etl;

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

import java.awt.*;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.gmu.stc.datavisualization.netcdf.PngFactory;
import edu.gmu.stc.hadoop.raster.DataChunk;
import edu.gmu.stc.hadoop.raster.io.datastructure.ArrayFloatSerializer;
import edu.gmu.stc.hadoop.raster.io.datastructure.ArrayIntSerializer;
import edu.gmu.stc.hadoop.raster.hdf5.H5Chunk;
import edu.gmu.stc.hadoop.raster.hdf5.H5FileInputFormat;
import edu.gmu.stc.hadoop.vector.Rectangle;
import edu.gmu.stc.hadoop.vector.extension.CountyFeature;
import edu.gmu.stc.spark.io.kryo.SparkKryoRegistrator;
import scala.Tuple2;

/**
 * Created by Fei Hu on 3/17/16.
 */
public class ETLToolTestCluster {

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
    //hconf.set("mapreduce.input.fileinputformat.inputdir", args[0]);
    hconf.set("mapreduce.input.fileinputformat.inputdir", "/Users/feihu/Documents/Data/Merra2/");
    hconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    hconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    hconf.setStrings("variables", vars);
    //hconf.setStrings("variables", args[0]);
    hconf.setStrings("bbox", "[0-1,0-361,0-576],[1-24,0-361,0-576]");
    hconf.setStrings("startTime", "19800101");
    hconf.setStrings("endTime", "20151201");

    //Did not use the following setting
    hconf.setStrings("datanodeNum", "14");
    hconf.setStrings("slotNum", "10");
    hconf.setStrings("threadNumPerNode", "10");

    final int width = 576, height = 364;
    final double xResolution = 360.0 / width; //MetaData.MERRA2.lonUnit;
    final double yResolution = 180.0 / height; //MetaData.MERRA2.latUnit;
    final double x_orig = -180.0; //MetaData.MERRA2.lon_orig;
    final double y_orig = -90.0; //MetaData.MERRA2.lat_orig;
    final int interplateScale = 1;
    final int pngScale = 4;

    final String[] stateNames = new String[]{"Alaska", "Hawaii", "Puerto"}; //new String[]{"Alaska", "Hawaii", "Puerto"};
    final boolean isObject = false;

    boolean isGlobal = true;
    if (isGlobal) {
      double x_min = Double.MAX_VALUE, x_max = -1*Double.MAX_VALUE, y_min = Double.MAX_VALUE, y_max = -1*Double.MAX_VALUE;
      Rectangle queryBBox = new Rectangle(x_min,y_min,x_max,y_max);
      hconf.set("geoBBox", queryBBox.toWKT());
      queryBBox = new Rectangle(-185,-95,185,95);
      hconf.set("geoBBox", queryBBox.toWKT());
      int[] globalmask = new int[height*width];
      for (int i= 0; i<height; i++) {
        for (int j=0; j<width; j++) {
          globalmask[i*width+j] = 1;
        }
      }
      H5Chunk chunk = new H5Chunk();
      chunk.setShape(new int[]{height, width});
      chunk.setCorner(new int[]{0, 0});
      Tuple2<H5Chunk, ArrayIntSerializer> maskLocal = new Tuple2<H5Chunk, ArrayIntSerializer>(chunk, new ArrayIntSerializer(new int[]{height, width}, globalmask));
    } else {
      //JavaRDD<String> geoJson = sc.textFile(args[1]).filter(new GeoExtracting.GeoJSONFilter(stateNames, isObject));
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
    }
    //JavaRDD<String> geoJson = sc.textFile(args[1]).filter(new GeoExtracting.GeoJSONFilter(stateNames, isObject));
    //JavaRDD<String> geoJson = sc.textFile("/Users/feihu/Desktop/gz_2010_us_040_00_500k.json").filter(new GeoExtracting.GeoJSONFilter(stateNames, isObject));

    //JavaPairRDD<String, CountyFeature> countyRDD = geoJson.mapToPair(new GeoExtracting.GeoFeatureFactory());

    /*JavaRDD<CountyFeature> states = countyRDD.reduceByKey(
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
    });*/

    //List<Rectangle> statesBList = stateBoundaries.collect();
    double x_min = Double.MAX_VALUE, x_max = -1*Double.MAX_VALUE, y_min = Double.MAX_VALUE, y_max = -1*Double.MAX_VALUE;
    /*for (Rectangle rectangle : statesBList) {
      x_min = Math.min(x_min, rectangle.getMinX());
      x_max = Math.max(x_max, rectangle.getMaxX());
      y_min = Math.min(y_min, rectangle.getMinY());
      y_max = Math.max(y_max, rectangle.getMaxY());
    }*/

    Rectangle queryBBox = new Rectangle(x_min,y_min,x_max,y_max);
    hconf.set("geoBBox", queryBBox.toWKT());

    //JavaPairRDD<H5Chunk, ArrayIntSerializer> stateMask = states.mapToPair( new GeoExtracting.ChunkMaskFactory(xResolution, yResolution, x_orig, y_orig));

    //List<Tuple2<H5Chunk, ArrayIntSerializer>> maskList = stateMask.collect();

    //Tuple2<H5Chunk, ArrayIntSerializer> maskLocal = GeoExtracting.combineChunkMasks(maskList);

    queryBBox = new Rectangle(-185,-95,185,95);
    hconf.set("geoBBox", queryBBox.toWKT());
    int[] globalmask = new int[height*width];
    for (int i= 0; i<height; i++) {
      for (int j=0; j<width; j++) {
        globalmask[i*width+j] = 1;
      }
    }

    H5Chunk chunk = new H5Chunk();
    chunk.setShape(new int[]{height, width});
    chunk.setCorner(new int[]{0, 0});
    Tuple2<H5Chunk, ArrayIntSerializer> maskLocal = new Tuple2<H5Chunk, ArrayIntSerializer>(chunk, new ArrayIntSerializer(new int[]{height, width}, globalmask));

    PngFactory.drawPNG(maskLocal._2().getArray(), "/Users/feihu/Desktop/test/boundary" + ".png", 0.0f, 1.0f, null, pngScale);

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
        recordsWithCoordinateChanging.mapToPair(new GeoExtracting.InterpolateArrayByIDW(interplateScale))
            .mapToPair(new PairFunction<Tuple2<DataChunk, ArrayFloatSerializer>, String, Tuple2<DataChunk, ArrayFloatSerializer>>() {
              @Override
              public Tuple2<String, Tuple2<DataChunk, ArrayFloatSerializer>> call(
                  Tuple2<DataChunk, ArrayFloatSerializer> tuple2) throws Exception {
                String key = tuple2._1.getVarShortName() + "_" + tuple2._1().getFilePath().split("\\.")[2] + "_" + tuple2._1().getCorner()[0];
                return new Tuple2<String, Tuple2<DataChunk, ArrayFloatSerializer>>(key, tuple2);
              }
            });

    JavaPairRDD<String, ArrayFloatSerializer> combinedChunks = rddCombinedByVarAndTime.groupByKey().mapToPair( new GeoExtracting.CombineChunks(mask, interplateScale));


    JavaPairRDD<String, Tuple2<String, ArrayFloatSerializer>> timeChunks = combinedChunks.mapToPair(
        new PairFunction<Tuple2<String, ArrayFloatSerializer>, String, Tuple2<String, ArrayFloatSerializer>>() {
          @Override
          public Tuple2<String, Tuple2<String, ArrayFloatSerializer>> call(
              Tuple2<String, ArrayFloatSerializer> tuple2) throws Exception {
            String[] tmps = tuple2._1().split("_");
            String hour = String.format("%02d", Integer.parseInt(tmps[2]));
            return new Tuple2<String, Tuple2<String, ArrayFloatSerializer>>(tmps[0]+tmps[1], new Tuple2<String, ArrayFloatSerializer>(tmps[0]+"_"+tmps[1]+"_"+hour, tuple2._2()));
          }
        });

    timeChunks.groupByKey().foreach(
        new VoidFunction<Tuple2<String, Iterable<Tuple2<String, ArrayFloatSerializer>>>>() {
          @Override
          public void call(Tuple2<String, Iterable<Tuple2<String, ArrayFloatSerializer>>> tuple2) throws Exception {
            ArrayList<Image> images = new ArrayList<Image>();
            for (int i=0; i<24; i++) {
              images.add(new BufferedImage(1,1,1));
            }

            Iterator<Tuple2<String, ArrayFloatSerializer>> itor = tuple2._2().iterator();

            while (itor.hasNext()) {
              Tuple2<String, ArrayFloatSerializer> tuple = itor.next();
              Image image = PngFactory.getImage(tuple._2().getArray(), 107.2249f, 319.2336f, tuple._1(), pngScale);
              int index = Integer.parseInt(tuple._1().split("_")[2]);
              images.set(index, image);
            }
            PngFactory.geneGIFBilinear(images, "/Users/feihu/Desktop/test/" + tuple2._1(), 1, 500);
          }
        });

    timeChunks.foreach(new VoidFunction<Tuple2<String, Tuple2<String, ArrayFloatSerializer>>>() {
      @Override
      public void call(Tuple2<String, Tuple2<String, ArrayFloatSerializer>> tuple2) throws Exception {
        Tuple2<String, ArrayFloatSerializer> tuple = tuple2._2();
        PngFactory.drawPNG(tuple._2().getArray(), "/Users/feihu/Desktop/test/"+ tuple._1() + ".png", 107.2249f, 319.2336f, tuple._1(), pngScale);
      }
    });
  }

}
