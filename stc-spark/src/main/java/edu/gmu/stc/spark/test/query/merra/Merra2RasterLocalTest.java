package edu.gmu.stc.spark.test.query.merra;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.hadoop.raster.DataChunk;
import edu.gmu.stc.hadoop.raster.io.datastructure.ArrayFloatSerializer;
import edu.gmu.stc.hadoop.raster.hdf5.H5FileInputFormat;
import scala.Tuple2;
import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import ucar.ma2.Index;
import ucar.ma2.Range;

/**
 * Created by Fei Hu on 3/10/16.
 */
public class Merra2RasterLocalTest {

  public static void main(String[] args) throws ClassNotFoundException {
    SparkConf sconf = new SparkConf().setAppName("SparkTest").setMaster("local[4]").registerKryoClasses(new Class<?>[]{
        Class.forName("org.apache.hadoop.io.LongWritable")});
    JavaSparkContext sc = new JavaSparkContext(sconf);

    SQLContext sqlContext = new SQLContext(sc);
    Configuration hconf = new Configuration();
    String vars = "UFLXKE,AUTCNVRN,BKGERR";

    hconf.set("mapreduce.input.fileinputformat.inputdir", "/Users/feihu/Documents/Data/Merra2/");

    hconf.setStrings("variables", vars);
    //hconf.setStrings("variables", args[0]);
    hconf.setStrings("bbox", "[0-1,0-361,0-540]");
    hconf.setStrings("startTime", "19800101");
    hconf.setStrings("endTime", "20151201");
    hconf.setStrings("datanodeNum", "14");
    hconf.setStrings("slotNum", "10");
    hconf.setStrings("threadNumPerNode", "10");

    JavaPairRDD<DataChunk, ArrayFloatSerializer> records = sc.newAPIHadoopRDD(hconf,
                                                                              H5FileInputFormat.class,
                                                                              DataChunk.class,
                                                                              ArrayFloatSerializer.class);

    //the upside of y are change to downside
    JavaPairRDD<DataChunk, ArrayFloatSerializer> recordsWithCordinateChanging = records.mapValues(
        new Function<ArrayFloatSerializer, ArrayFloatSerializer>() {
          @Override
          public ArrayFloatSerializer call(ArrayFloatSerializer v1) throws Exception {
            ArrayFloat result = (ArrayFloat) Array.factory(float.class, v1.getArray().getShape());
            Index index = Index.factory(result.getShape());
            Index rawIndex = Index.factory(result.getShape());
            switch (index.getRank()) {
              case 3:
                int ysize = result.getShape()[1];
                for (int t=0; t<index.getShape(0); t++) {
                  for (int y=0; y<index.getShape(1); y++) {
                    for (int x=0; x<index.getShape(2); x++) {
                      index.set(t,y,x);
                      result.set(index,v1.getArray().get(rawIndex.set(t, rawIndex.getShape(1)-y-1, x)));
                    }
                  }
                }
                break;
            }
            return new ArrayFloatSerializer(result.getShape(), (float[]) result.getStorage());
          }
        });


    records.foreach(new VoidFunction<Tuple2<DataChunk, ArrayFloatSerializer>>() {
      @Override
      public void call(Tuple2<DataChunk, ArrayFloatSerializer> tuple) throws Exception {
        System.out.println(tuple._1.toString());
        ArrayFloat array = tuple._2().getArray();
        List<Range> rangeList = new ArrayList<Range>();
        rangeList.add(new Range(0, 0, 1));
        rangeList.add(new Range(10, 20, 1));
        rangeList.add(new Range(10, 20, 1));

        ArrayFloat value = (ArrayFloat) array.section(rangeList);
        System.out.println(value.getSize());
      }
    });
    System.out.println(records.count());

    /*
    JavaPairRDD<DataChunk, ArrayFloatSerializer> reducedRDD = records.reduceByKey(
        new Function2<ArrayFloatSerializer, ArrayFloatSerializer, ArrayFloatSerializer>() {
          @Override
          public ArrayFloatSerializer call(ArrayFloatSerializer v1, ArrayFloatSerializer v2)
              throws Exception {
            return v1;
          }
        });

    reducedRDD.foreach(new VoidFunction<Tuple2<DataChunk, ArrayFloatSerializer>>() {
      @Override
      public void call(Tuple2<DataChunk, ArrayFloatSerializer> tuple2) throws Exception {
        System.out.println(tuple2._1().toString());
      }
    });*/

  }

}
