package edu.gmu.stc.spark.test.query.merra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import edu.gmu.stc.hadoop.index.io.input.reading.MerraInputFormatByDBIndexWithScheduler;
import edu.gmu.stc.hadoop.index.io.merra.VariableInfo;
import edu.gmu.stc.spark.test.query.merra.io.MerraVar;
import edu.gmu.stc.spark.test.query.merra.udf.SpaceQuery;
import scala.Tuple2;
import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;


/**
 * Created by Fei Hu on 1/29/16.
 */
public class MerraSQLTest {
  private static final Log LOG = LogFactory.getLog(MerraSQLTest.class);

  public static void main(String[] args) {
    SparkConf sconf = new SparkConf().setAppName("SparkTest").setMaster("local[4]");
    JavaSparkContext sc = new JavaSparkContext(sconf);

    SQLContext sqlContext = new SQLContext(sc);
    Configuration hconf = new Configuration();

    String vars = "GRN,LAI";  //BASEFLOW,ECHANGE,EVLAND,EVPINTR,EVPSBLN,EVPSOIL,EVPTRNS,

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

    LOG.info("++++++++++ partition :  " + records.partitions().get(0).hashCode());

    JavaPairRDD<VariableInfo, List<Float>> merra = records.mapToPair(new PairFunction<Tuple2<VariableInfo, Array>,
        VariableInfo, List<Float>>() {
      @Override
      public Tuple2<VariableInfo, List<Float>> call(Tuple2<VariableInfo, Array> input) throws Exception {
        List<Float> vList = new ArrayList<Float>();
        for (int i=0; i<input._2().getSize(); i++) {
          vList.add(input._2().getFloat(i));
        }
        return new Tuple2<VariableInfo, List<Float>>(input._1(), vList);
      }
    });

    JavaRDD<MerraVar> merraVarJavaRDD = merra.mapPartitions(
        new FlatMapFunction<Iterator<Tuple2<VariableInfo, List<Float>>>, MerraVar>() {
          @Override
          public Iterable<MerraVar> call(Iterator<Tuple2<VariableInfo, List<Float>>> vars)
              throws Exception {
            List<MerraVar> varList = new ArrayList<MerraVar>();
            while (vars.hasNext()) {
              Tuple2<VariableInfo, List<Float>> merraTuple = vars.next();
              List<Integer> shape = new ArrayList<Integer>();
              shape.add(361);
              shape.add(540);
              varList.add(new MerraVar(merraTuple._1().getShortName(), merraTuple._1().getTime(), shape, merraTuple._2()));
            }
            return varList;
          }
        }, true);

    //sqlContext.udf().register("SpaceQuery", new SpaceQuery(), DataTypes.createArrayType(DataTypes.FloatType));
    sqlContext.udf().register("spacequery", new SpaceQuery(), DataTypes.createArrayType(DataTypes.FloatType));
    DataFrame merraFrame = sqlContext.createDataFrame(merraVarJavaRDD, MerraVar.class);

    merraFrame.registerTempTable("Merra");

    //sqlContext.sql("Select * From Merra Where values[10] > 0").show();
    sqlContext.sql("Select spacequery(values,'0-0', '100-200'), shape, shortName, time From Merra").show();

    merraFrame.printSchema();

  }

}
