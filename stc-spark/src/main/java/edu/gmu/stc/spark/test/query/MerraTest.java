package edu.gmu.stc.spark.test.query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

/**
 * Created by feihu on 1/19/16.
 */
public class MerraTest {
    private static final Log LOG = LogFactory.getLog(MerraTest.class);

    public static void main(String[] args) {
        SparkConf sconf = new SparkConf().setAppName("SparkTest").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(sconf);
        JavaRDD<Integer> numRDD = sc.parallelize(Arrays.asList(1,2,3,4)) ;
        numRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                LOG.info(integer);
            }
        });


    }
}
