package edu.gmu.stc.spark.test.query.modis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import edu.gmu.stc.spark.io.kryo.SparkKryoRegistrator;

/**
 * Created by Fei Hu on 10/18/16.
 */
public class CellSQLOperator {

  public static void main(String[] args) {
    if (args.length != 3) {
      System.out.print("please input the configuration file, and outputfilepath");
      return;
    }

    String parquetFilePath = args[0];
    String talbeName = args[1];
    String querySQL = args[2];

    final SparkConf sconf = new SparkConf().setAppName(CellSQLOperator.class.getName() + querySQL);
    sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sconf.set("spark.kryo.registrator", SparkKryoRegistrator.class.getName());

    JavaSparkContext sc = new JavaSparkContext(sconf);
    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
    DataFrame df = sqlContext.read().parquet(parquetFilePath);
    df.registerTempTable(talbeName);
    sqlContext.sql(querySQL).show();
  }

}
