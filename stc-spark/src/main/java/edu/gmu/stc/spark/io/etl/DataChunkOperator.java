package edu.gmu.stc.spark.io.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import edu.gmu.stc.configure.MyProperty;
import edu.gmu.stc.hadoop.raster.DataChunk;
import edu.gmu.stc.hadoop.raster.DataChunkInputFormat;
import edu.gmu.stc.hadoop.raster.io.datastructure.ArrayFloatSerializer;
import edu.gmu.stc.hadoop.raster.io.datastructure.ArrayIntSerializer;
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer;
import edu.gmu.stc.hadoop.raster.io.datastructure.ArrayShortSerializer;
import edu.gmu.stc.spark.io.kryo.SparkKryoRegistrator;
import scala.Tuple2;
import ucar.ma2.Array;
import ucar.ma2.Index;

/**
 * Created by Fei Hu on 8/26/16.
 */
public class DataChunkOperator {

  public static void main(String[] args) {

    String inputDir = "";
    int start_time = 2016002; //conf.getInt("start_time", 0);
    int end_time = 2016002; //conf.getInt("end_time", 0);
    String[] var_shortNames = new String[]{"Aerosol_Optical_Depth_Land_Ocean_Mean"}; //conf.getStrings("var_shortNames");
    String[] geometryInfos = new String[]{"0"}; // conf.getStrings("geometryinfo");
    String filePath_prefix = "/Users/feihu/Documents/Data/modis_hdf/MOD08_D3"; //conf.get("filePath_prefix");
    String filePath_suffix = ".hdf"; //conf.get("filePath_suffix");


    final SparkConf sconf = new SparkConf().setAppName(DataChunkOperator.class.getName()).setMaster("local[6]");
    sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sconf.set("spark.kryo.registrator", SparkKryoRegistrator.class.getName());

    JavaSparkContext sc = new JavaSparkContext(sconf);
    Configuration hconf = new Configuration();
    hconf.set("fs.defaultFS", MyProperty.nameNode);
    hconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    hconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    hconf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
    hconf.set("mapreduce.input.fileinputformat.inputdir", inputDir);
    hconf.setInt("start_time", start_time);
    hconf.setInt("end_time", end_time);
    hconf.setStrings("var_shortNames", var_shortNames);
    hconf.setStrings("geometryinfo", geometryInfos);
    hconf.set("filePath_prefix", filePath_prefix);
    hconf.set("filePath_suffix", filePath_suffix);

    JavaPairRDD<DataChunk, ArraySerializer> records = sc.newAPIHadoopRDD(hconf, DataChunkInputFormat.class,
                                                                         DataChunk.class,
                                                                         ArraySerializer.class);
    records.foreach(new VoidFunction<Tuple2<DataChunk, ArraySerializer>>() {
      @Override
      public void call(Tuple2<DataChunk, ArraySerializer> tuple2) throws Exception {
        Array array = tuple2._2().getArray().section(new int[]{90, 45}, new int[]{10,10});
        for (short i : (short[]) array.get1DJavaArray(short.class)) {
          System.out.println(i);
        }

      }
    });

  }

}
