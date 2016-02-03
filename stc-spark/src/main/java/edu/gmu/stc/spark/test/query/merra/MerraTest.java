package edu.gmu.stc.spark.test.query.merra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.gmu.stc.spark.test.query.merra.io.MerraVar;

/**
 * Created by feihu on 1/19/16.
 */
public class MerraTest {
    private static final Log LOG = LogFactory.getLog(MerraTest.class);

    public static void main(String[] args) {
        SparkConf sconf = new SparkConf().setAppName("SparkTest").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(sconf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<Integer> numRDD = sc.parallelize(Arrays.asList(1,2,3,4)) ;
        JavaRDD<MerraVar> arrayRDD = numRDD.map(new Function<Integer, MerraVar>() {
          @Override
          public MerraVar call(Integer integer) throws Exception {
            List<Integer> shape = new ArrayList<Integer>();
            shape.add(361);
            shape.add(540);
            List<Float> value = new ArrayList<Float>();
            value.add(361.1F);
            value.add(540.1F);
            return new MerraVar("Test", 198001, shape, value);
          }
        });

      DataFrame merraFrame = sqlContext.createDataFrame(arrayRDD, MerraVar.class);

      merraFrame.registerTempTable("Merra");

      merraFrame.printSchema();

      sqlContext.sql("Select values From Merra").show();

      numRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                LOG.info(integer);
            }
        });
    }
}
