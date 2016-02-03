package edu.gmu.stc.spark.test.query.merra;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SQLContext;
//import scala.float;
import org.apache.spark.storage.StorageLevel;

import edu.gmu.stc.configure.MyProperty;
import edu.gmu.stc.hadoop.index.io.input.reading.MerraInputFormatByDBIndexWithScheduler;
import edu.gmu.stc.hadoop.index.io.merra.VariableInfo;
import scala.Tuple2;
import ucar.ma2.Array;

import java.util.HashMap;
import java.util.List;

/**
 * Created by feihu on 12/31/15.
 */
public class MerraSDTest {
    private static final Log LOG = LogFactory.getLog(MerraSDTest.class);

    public static void main(String[] args) {
        if (args.length != 8) {
            System.out.println("Please input the right number of input parameters.");
            System.err.println( "Please input 8 input variables: inputPath, variables, bbox, starttime, endtime, datanodeNum, slotNum, threadNumPerNode");
            System.exit(2);
        }

        SparkConf sconf = new SparkConf().setAppName("SparkTest");//.setMaster("spark://Feis-MBP.home:6066"); //local[4]
        //SparkConf sconf = new SparkConf().setAppName("SparkTest").setMaster("local[4]");

        JavaSparkContext sc = new JavaSparkContext(sconf);

        SQLContext sqlContext = new SQLContext(sc);
        Configuration hconf = new Configuration();
        hconf.addResource(new Path(MyProperty.hadoopHome + "/core-site.xml"));
        hconf.addResource(new Path(MyProperty.hadoopHome+"/hdfs-site.xml"));

        hconf.set("mapreduce.framework.name", "yarn");
        hconf.set("mapreduce.jobhistory.address", "SERVER-A8-C-U26:10020");

        hconf.set("yarn.app.mapreduce.am.command-opts", "-Xmx1024m");

        hconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        hconf.set("yarn.resourcemanager.address", "10.8.2.26:8032");

        hconf.set("yarn.resourcemanager.resource-tracker.address", "10.8.2.26:8031");
        hconf.set("yarn.resourcemanager.scheduler.address", "10.8.2.26:8030");
        hconf.set("yarn.resourcemanager.admin.address", "10.8.2.26:8033");


        hconf.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");

        hconf.set("yarn.application.classpath", "/etc/hadoop/conf.cloudera.hdfs,"
                + "/etc/hadoop/conf.cloudera.yarn,"
                + "/opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/lib/hadoop/*,"
                + "/opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/lib/hadoop/lib/*,"
                + "/opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/lib/hadoop-hdfs/*,"
                + "/opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/lib/hadoop-hdfs/lib/*,"
                + "/opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/lib/hadoop-yarn/*,"
                + "/opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/lib/hadoop-yarn/lib/*");



        String vars = "GRN,LAI";
        //hconf.set("mapreduce.input.fileinputformat.inputdir", "/Users/feihu/Documents/Data/Merra/");
        //hconf.set("mapreduce.input.fileinputformat.inputdir", "hdfs://SERVER-A8-C-U26:8020/Merra/MERRA_MONTHLY/MSTMNXMLD.5.2.0/");
        hconf.set("fs.defaultFS", "hdfs://SERVER-A8-C-U26:8020");

        //hconf.setStrings("variables", vars);
        hconf.set("mapreduce.input.fileinputformat.inputdir", args[0]);
        hconf.setStrings("variables", args[1]);
        hconf.setStrings("bbox", args[2]); //"[0-1,0-361,0-540]"
        hconf.setStrings("startTime", args[3]); //"198001"
        hconf.setStrings("endTime", args[4]);  //201512
        hconf.setStrings("datanodeNum", args[5]);   //14
        hconf.setStrings("slotNum", args[6]);  //10
        hconf.setStrings("threadNumPerNode", args[7]); //10

        JavaPairRDD<VariableInfo, Array>
                records = sc.newAPIHadoopRDD(hconf,
                                             MerraInputFormatByDBIndexWithScheduler.class,
                                             VariableInfo.class, Array.class);

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
                LOG.info(input._1() + " : " + input._2()._1());
            }
        });

        //sqrMeanSum.saveAsTextFile("/result.txt");

        //System.out.println(bMeanList.getValue().get(0)._2()[1]);
    }
}
