package edu.gmu.stc.spark.test.query;

import edu.gmu.stc.hadoop.index.io.input.reading.MerraInputFormatByDBIndexWithScheduler;
import edu.gmu.stc.hadoop.index.io.merra.VariableInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import ucar.ma2.Array;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by feihu on 1/19/16.
 */
public class MerraSDLocalTest {
    private static final Log LOG = LogFactory.getLog(MerraSDLocalTest.class);

    public static void main(String[] args) {
        SparkConf sconf = new SparkConf().setAppName("SparkTest").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(sconf);

        SQLContext sqlContext = new SQLContext(sc);
        Configuration hconf = new Configuration();


        String vars = "GRN,LAI";  //"BASEFLOW,ECHANGE,EVLAND,EVPINTR,EVPSBLN,EVPSOIL,EVPTRNS,
                                  // FRSAT,FRSNO,FRUNST,FRWLT,GHLAND,GRN,GWETPROF,GWETROOT,
                                   //GWETTOP,LAI,LHLAND,LWLAND,PARDF,PARDR,PRECSNO,PRECTOT,
                                    // PRMC,QINFIL,RUNOFF,RZMC,SFMC,SHLAND,SMLAND,SNODP,SNOMAS,
                                    // SPLAND,SPSNOW,SPWATR,SWLAND,TELAND,TPSNOW,TSAT,TSOIL1
                                    // TSOIL2,TSOIL3,TSOIL4,TSOIL5,TSOIL6,TSURF,TUNST,TWLAND,
                                    // TWLT"

        //Daily variables:
        //DMDT_DYN,DMDT_ANA,DQVDT_DYN,DQVDT_PHY,DQVDT_ANA,DQVDT_MST,DQVDT_TRB,DQVDT_CHM,DQLDT_DYN,DQLDT_PHY,
        // DQLDT_ANA,DQLDT_MST,DQIDT_DYN,DQIDT_PHY,DQIDT_ANA,DQIDT_MST,DOXDT_DYN,DOXDT_PHY,DOXDT_ANA,DOXDT_CHM,
        // DKDT_DYN,DKDT_PHY,DKDT_ANA,DKDT_PHYPHY,DHDT_DYN,DHDT_PHY,DHDT_ANA,DHDT_RES,DPDT_DYN,DPDT_PHY,
        // DPDT_ANA,UFLXCPT,VFLXCPT,UFLXPHI,VFLXPHI,UFLXKE,VFLXKE,UFLXQV,VFLXQV,UFLXQL,
        // VFLXQL,UFLXQI,VFLXQI,TEFIXER,CONVCPT,CONVPHI,CONVKE,CONVTHV,DKDT_GEN,DKDT_PG,
        // DKDT_REMAP,DKDT_INRES,DKDT_PGRES,DHDT_REMAP,DPDT_REMAP,DKDT_GWD,DKDT_RAY,DKDT_BKG,DKDT_ORO,DKDT_GWDRES,
        // BKGERR,DHDT_GWD,DHDT_RAY,DHDT_BKG,DHDT_ORO,DKDT_TRB,DKDT_SRF,DKDT_INT,DKDT_TOP,DKDT_MST,
        // DHDT_TRB,DHDT_MST,DHDT_FRI,DHDT_RAD,DHDT_CUF,QTFILL,DQVDT_FIL,DQIDT_FIL,DQLDT_FIL,DOXDT_FIL,
        // HFLUX,EVAP,PRECCU,PRECLS,PRECSN,DTHDT_ANA,DTHDT_PHY,DTHDT_DYN,DTHDT_REMAP,DTHDT_CONSV,
        // DTHDT_FIL,LWTNET,LWGNET,SWNETTOA,SWNETSRF,LSCNVCL,LSCNVCI,LSCNVRN,CUCNVCL,CUCNVCI,
        // CUCNVRN,EVPCL,EVPRN,SUBCI,SUBSN,AUTCNVRN,SDMCI,COLCNVRN,COLCNVSN,FRZCL,FRZRN

        hconf.set("mapreduce.input.fileinputformat.inputdir", "/Users/feihu/Documents/Data/Merra/");

        hconf.setStrings("variables", vars);
        //hconf.setStrings("variables", args[0]);
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

        merra.persist(StorageLevel.MEMORY_AND_DISK());

        JavaPairRDD<String, Tuple2<Integer, float[]>> merraByVar = merra.mapToPair(new PairFunction<Tuple2<VariableInfo, float[]>, String, Tuple2<Integer, float[]>>() {
            @Override
            public Tuple2<String, Tuple2<Integer, float[]>> call(Tuple2<VariableInfo, float[]> input) throws Exception {
                return new Tuple2<String, Tuple2<Integer, float[]>>(input._1().getShortName(),
                        new Tuple2<Integer, float[]>(1, input._2()));
            }
        });

        System.out.println("merraByVar size : " + merraByVar.partitions().size());

        //get the sum for each variable
        JavaPairRDD<String, Tuple2<Integer, float[]>> sum = merraByVar.reduceByKey(new Function2<Tuple2<Integer, float[]>, Tuple2<Integer, float[]>, Tuple2<Integer, float[]>>() {
            @Override
            public Tuple2<Integer, float[]> call(Tuple2<Integer, float[]> v1, Tuple2<Integer, float[]> v2) throws Exception {
                float[] r = new float[v1._2().length];
                for (int i =0; i<v1._2().length; i++) {
                    r[i] = v1._2()[i] + v2._2()[i];
                }
                return new Tuple2<Integer, float[]>(v1._1()+v2._1(), r);
            }
        });

        //get the mean for each variable
        JavaPairRDD<String, float[]> mean = sum.mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, float[]>>, String, float[]>() {
            @Override
            public Tuple2<String, float[]> call(Tuple2<String, Tuple2<Integer, float[]>> input) throws Exception {
                float[] r = new float[input._2()._2().length];
                for(int i=0; i<input._2()._2().length; i++) {
                    r[i] = input._2()._2()[i]/input._2()._1();
                };

                return new Tuple2<String, float[]>(input._1(), r);
            }
        });

        List<Tuple2<String, float[]>> meanList = mean.collect();
        HashMap<String, float[]> varMean = new HashMap<String, float[]>();
        for (Tuple2<String, float[]> pair : meanList) {
            varMean.put(pair._1(), pair._2());
        }

        final Broadcast<HashMap<String, float[]>> bVarMeanMap = sc.broadcast(varMean);

        JavaPairRDD<String, Tuple2<Integer, float[]>> sqrMean = merra.mapToPair(new PairFunction<Tuple2<VariableInfo, float[]>, String, Tuple2<Integer, float[]>>() {
            @Override
            public Tuple2<String, Tuple2<Integer, float[]>> call(Tuple2<VariableInfo, float[]> input) throws Exception {
                float[] mean = bVarMeanMap.getValue().get(input._1().getShortName());
                float[] r = new float[mean.length];
                for(int i=0; i < mean.length; i++) {
                    r[i] = (mean[i] - input._2()[i]) * (mean[i] - input._2()[i]);
                }

                return new Tuple2<String, Tuple2<Integer, float[]>> (input._1().getShortName(), new Tuple2<Integer, float[]>(1, r));
            }
        });


        JavaPairRDD<String, Tuple2<Integer, float[]>> sqrMeanSum = sqrMean.reduceByKey(new Function2<Tuple2<Integer, float[]>, Tuple2<Integer, float[]>, Tuple2<Integer, float[]>>() {
            @Override
            public Tuple2<Integer, float[]> call(Tuple2<Integer, float[]> v1, Tuple2<Integer, float[]> v2) throws Exception {
                int num = v1._1() + v2._1();
                float[] r = new float[v1._2().length];
                for (int i=0; i<r.length; i++) {
                    r[i] = v1._2()[i] + v2._2()[i];
                }
                return new Tuple2<Integer, float[]>(num, r);
            }
        });

        sqrMeanSum.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, float[]>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Integer, float[]>> input) throws Exception {
                LOG.info(input._1() + " : **************** " + input._2()._1());
            }
        });

        //sqrMeanSum.saveAsTextFile("./result.txt");

        //just for test for merge from different branches

        //System.out.println(bMeanList.getValue().get(0)._2()[1]);

        //target test
    }
}
