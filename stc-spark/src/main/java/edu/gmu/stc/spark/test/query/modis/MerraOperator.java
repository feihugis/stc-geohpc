package edu.gmu.stc.spark.test.query.modis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
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
import ucar.ma2.ArrayFloat;
import ucar.ma2.ArrayShort;

/**
 * Created by Fei Hu on 10/17/16.
 */
public class MerraOperator {
  public static void main(String[] args) {
    if (args.length != 2) {
      System.out.print("please input the configuration file, and outputfilepath");
      return;
    }

    String configFilePath = args[0];
    String outputFilePath = args[1];

    final SparkConf sconf = new SparkConf().setAppName(ModisOperator.class.getName());
    sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sconf.set("spark.kryo.registrator", SparkKryoRegistrator.class.getName());

    JavaSparkContext sc = new JavaSparkContext(sconf);
    Configuration hconf = new Configuration();
    hconf.addResource(new Path(configFilePath));

    JavaPairRDD<DataChunk, ArraySerializer> climateRDD = sc.newAPIHadoopRDD(hconf, DataChunkInputFormat.class,
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

    JavaRDD<Cell> cellRDD = climateRDD.flatMap(
        new FlatMapFunction<Tuple2<DataChunk, ArraySerializer>, Cell>() {
          @Override
          public Iterable<Cell> call(Tuple2<DataChunk, ArraySerializer> tuple2)
              throws Exception {
            List<Cell> cellList = new ArrayList<Cell>();
            DataChunk dataChunk = tuple2._1;
            int[] corner = dataChunk.getCorner();
            int[] shape = dataChunk.getShape();
            ArrayFloat array = (ArrayFloat) tuple2._2.getArray();

            for (int t = 0; t < shape[0]; t++) {
              for (int y = 0; y < shape[1]; y++) {
                for (int x = 0; x < shape[2]; x++) {
                  float value = array.getFloat(t * shape[1] * shape[2] + y * shape[2] + x);
                  cellList.add(new Cell(dataChunk.getVarShortName(),
                                        dataChunk.getTime(),
                                        corner[1]+y, corner[2]+x,
                                        value, "n"));
                }
              }
            }
            return cellList;
          }
        });

    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

    DataFrame db = sqlContext.createDataFrame(cellRDD, Cell.class);
    db.saveAsParquetFile(outputFilePath);
  }
}
