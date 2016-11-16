package edu.gmu.stc.spark.test.query.modis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.hadoop.raster.Cell;
import edu.gmu.stc.hadoop.raster.DataChunk;
import edu.gmu.stc.hadoop.raster.DataChunkInputFormat;
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer;
import edu.gmu.stc.spark.io.kryo.SparkKryoRegistrator;
import scala.Tuple2;
import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import ucar.ma2.IndexIterator;

/**
 * Created by Fei Hu on 10/31/16.
 */
public class MerraMean {
  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.print("please input the configuration file");
      return;
    }

    String configFilePath = args[0];

    final SparkConf sconf = new SparkConf().setAppName(ModisOperator.class.getName());
    sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sconf.set("spark.kryo.registrator", SparkKryoRegistrator.class.getName());

    JavaSparkContext sc = new JavaSparkContext(sconf);
    Configuration hconf = new Configuration();
    hconf.addResource(new Path(configFilePath));

    JavaPairRDD<DataChunk, ArraySerializer>
        climateRDD = sc.newAPIHadoopRDD(hconf, DataChunkInputFormat.class,
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

    JavaPairRDD<String, Tuple2<Float, Integer>> intermediaRDD = climateRDD.mapToPair(
        new PairFunction<Tuple2<DataChunk, ArraySerializer>, String, Tuple2<Float, Integer>>() {
          @Override
          public Tuple2<String, Tuple2<Float, Integer>> call(
              Tuple2<DataChunk, ArraySerializer> climateRDD)
              throws Exception {
            String key = climateRDD._1.getVarShortName() + "_" + climateRDD._1.getTime();
            int count = 0;
            float value = 0.0f;
            ArrayFloat array = (ArrayFloat) climateRDD._2.getArray();
            IndexIterator itor = array.getIndexIterator();
            while (itor.hasNext()) {
              value = value + itor.getFloatNext();
              count++;
            }

            return new Tuple2<String, Tuple2<Float, Integer>>(key, new Tuple2<Float, Integer>(value, count));
          }
        });

    JavaPairRDD<String, Float> resultRDD = intermediaRDD.reduceByKey(
        new Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>>() {
          @Override
          public Tuple2<Float, Integer> call(Tuple2<Float, Integer> tuple1,
                                             Tuple2<Float, Integer> tuple2)
              throws Exception {
            Float value = tuple1._1 + tuple2._1;
            Integer count = tuple1._2 + tuple2._2;
            return new Tuple2<Float, Integer>(value, count);
          }
        }).mapValues(new Function<Tuple2<Float, Integer>, Float>() {
      @Override
      public Float call(Tuple2<Float, Integer> rdd) throws Exception {
        return rdd._1/rdd._2;
      }
    });

    List<Tuple2<String, Float>> results = resultRDD.collect();
    for (Tuple2<String, Float> result : results) {
      System.out.println(result._1 + " mean : " + result._2);
    }


  }
}
