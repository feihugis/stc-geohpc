package edu.gmu.stc.spark.test.query;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SQLContext;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;

import edu.gmu.stc.hadoop.index.io.input.reading.MerraInputFormatByDBIndexWithScheduler;
import edu.gmu.stc.hadoop.index.io.merra.VariableInfo;
import scala.Tuple2;
import ucar.ma2.Array;

/**
 * Created by Fei Hu on 1/29/16.
 */
public class MerraSQLTest {

  SparkConf sconf = new SparkConf().setAppName("SparkTest").setMaster("local[4]");
  JavaSparkContext sc = new JavaSparkContext(sconf);

  SQLContext sqlContext = new SQLContext(sc);
  Configuration hconf = new Configuration();


  String vars = "GRN,LAI";  //"BASEFLOW,ECHANGE,EVLAND,EVPINTR,EVPSBLN,EVPSOIL,EVPTRNS,

  hconf.set("mapreduce.input.fileinputformat.inputdir", "/Users/feihu/Documents/Data/Merra/");

  hconf.setStrings("variables", vars);
  hconf.setStrings("bbox", "[0-1,0-361,0-540]");
  hconf.setStrings("startTime", "198001");
  hconf.setStrings("endTime", "201512");
  hconf.setStrings("datanodeNum", "14");
  hconf.setStrings("slotNum", "10");
  hconf.setStrings("threadNumPerNode", "10");

  JavaPairRDD<VariableInfo, Array> records = sc.newAPIHadoopRDD(hconf,
                                                                MerraInputFormatByDBIndexWithScheduler.class,
                                                                VariableInfo.class,
                                                                Array.class);
  records.foreachPartition(new VoidFunction<Iterator<Tuple2<VariableInfo, Array>>>() {
    @Override
    public void call(Iterator<Tuple2<VariableInfo, Array>> tuple2Iterator) throws Exception {
      while (tuple2Iterator.hasNext()) {
        Tuple2<VariableInfo, Array> t = tuple2Iterator.next();
        LOG.info("++++++++++ partition info:  " + t._1().getShortName() + "_" + t._1().getTime() + "_" + t._1());
      }
    }
  });
  LOG.info("++++++++++ partition :  " + records.partitions().get(0).hashCode());

  JavaPairRDD<VariableInfo, float[]> merra = records.mapToPair(new PairFunction<Tuple2<VariableInfo, Array>,
      VariableInfo, float[]>() {
    @Override
    public Tuple2<VariableInfo, float[]> call(Tuple2<VariableInfo, Array> input) throws Exception {
      float[] values = new float[(int) input._2().getSize()];
      for (int i=0; i<input._2().getSize(); i++) {
        values[i] = input._2().getFloat(i);
      }
      return new Tuple2<VariableInfo, float[]>(input._1(), values);
    }
  });


}
