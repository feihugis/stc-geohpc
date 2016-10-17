package edu.gmu.stc.spark.io.etl;

import com.twitter.chill.AllScalaRegistrar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.List;

import edu.gmu.stc.hadoop.raster.DataChunk;
import edu.gmu.stc.hadoop.raster.DataChunkInputFormat;
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer;
import edu.gmu.stc.spark.io.kryo.SparkKryoRegistrator;
import scala.Tuple2;
import ucar.ma2.Array;
import ucar.ma2.DataType;

/**
 * Created by Fei Hu on 8/26/16.
 */
public class DataChunkOperator {

  public static void main(String[] args) {

    String configFilePath = args[0]; //"mod08-climatespark-config.xml"; //"merra100-climatespark-config.xml";

    final SparkConf sconf = new SparkConf().setAppName(DataChunkOperator.class.getName()).setMaster("local[6]");
    sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sconf.set("spark.kryo.registrator", SparkKryoRegistrator.class.getName());

    JavaSparkContext sc = new JavaSparkContext(sconf);
    Configuration hconf = new Configuration();
    hconf.addResource(new Path(configFilePath));

    JavaPairRDD<DataChunk, ArraySerializer> records = sc.newAPIHadoopRDD(hconf, DataChunkInputFormat.class,
                                                                         DataChunk.class,
                                                                         ArraySerializer.class).filter(
        new Function<Tuple2<DataChunk, ArraySerializer>, Boolean>() {
          @Override
          public Boolean call(Tuple2<DataChunk, ArraySerializer> v1) throws Exception {
            if (v1._1 != null) {
              return true;
            } else {
              return false;
            }
          }
        });

    records.foreach(new VoidFunction<Tuple2<DataChunk, ArraySerializer>>() {
      @Override
      public void call(Tuple2<DataChunk, ArraySerializer> tuple2) throws Exception {
        //System.out.println("shape ******** " + tuple2._1.getTime());
        if (tuple2._1() != null) {
          if (tuple2._1().getCorner()[0] == 0 && tuple2._1().getCorner()[1] == 0 && tuple2._1().getCorner()[2] == 0) {
            Array array = tuple2._2().getArray().section(new int[]{0, 0, 0}, new int[]{1,145,150});

            Class ValueType = DataType.getType(tuple2._1().getDataType()).getClassType();
            System.out.println(ValueType.getName());
            //Object v = ValueType.newInstance();
            for (short i : (short[]) array.get1DJavaArray(ValueType)) {
              //if (i < 100000000) {
                System.out.println( i);
              //}
            }
          }

        }
      }
    });

    List<Tuple2<DataChunk, ArraySerializer>> tuple2List =  records.collect();
    for (int i=0; i<tuple2List.size(); i++) {
      //System.out.println(tuple2List.get(i)._1().getVarShortName());
    }

    System.out.println("+++++++++++ " + records.collect().size());

  }

}
