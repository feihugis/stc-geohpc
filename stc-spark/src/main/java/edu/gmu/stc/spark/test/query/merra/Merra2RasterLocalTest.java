package edu.gmu.stc.spark.test.query.merra;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.hadoop.raster.DataChunk;
import edu.gmu.stc.hadoop.raster.hdf5.ArrayFloatSerializer;
import edu.gmu.stc.hadoop.raster.hdf5.H5FileInputFormat;
import scala.Tuple2;
import ucar.ma2.ArrayFloat;
import ucar.ma2.Range;

/**
 * Created by Fei Hu on 3/10/16.
 */
public class Merra2RasterLocalTest {

  public static void main(String[] args)  {
    SparkConf sconf = new SparkConf().setAppName("SparkTest").setMaster("local[4]");
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
    records.foreach(new VoidFunction<Tuple2<DataChunk, ArrayFloatSerializer>>() {
      @Override
      public void call(Tuple2<DataChunk, ArrayFloatSerializer> tuple) throws Exception {
        System.out.println(tuple._1.toString());
        ArrayFloat array = tuple._2().getArray();
        List<Range> rangeList = new ArrayList<Range>();
        rangeList.add(new Range(0,0,1));
        rangeList.add(new Range(10,20,1));
        rangeList.add(new Range(10,20,1));

        ArrayFloat value = (ArrayFloat) array.section(rangeList);
        System.out.println(value.getSize());
      }
    });
    System.out.println(records.count());
  }

}
