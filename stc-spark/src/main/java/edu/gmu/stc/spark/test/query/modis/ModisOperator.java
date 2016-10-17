package edu.gmu.stc.spark.test.query.modis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.hadoop.raster.Cell;
import edu.gmu.stc.hadoop.raster.DataChunk;
import edu.gmu.stc.hadoop.raster.DataChunkInputFormat;
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer;
import edu.gmu.stc.spark.io.kryo.SparkKryoRegistrator;
import scala.Tuple2;
import ucar.ma2.Array;
import ucar.ma2.ArrayShort;
import ucar.ma2.DataType;

/**
 * Created by Fei Hu on 9/7/16.
 */
public class ModisOperator {

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
            ArrayShort arrayShort = (ArrayShort) tuple2._2.getArray();

            for (int lat = corner[0]; lat< corner[0]+shape[0]; lat++) {
              for (int lon = corner[1]; lon < corner[1]+shape[1]; lon++) {
                float value = arrayShort.getFloat(lat*shape[1] + lon);
                cellList.add(new Cell(dataChunk.getVarShortName(), dataChunk.getTime(), lat, lon, value, "n"));
              }
            }
            return cellList;
          }
        });

    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

    DataFrame db = sqlContext.createDataFrame(cellRDD, Cell.class);
    db.saveAsParquetFile("/modis/mod08");
    //db.saveAsTable("", SaveMode.Append);
    //db.saveAsTable();

    /*cellRDD.foreach(new VoidFunction<Cell>() {
      @Override
      public void call(Cell cell) throws Exception {
        if (cell.getValue()>0) {
          System.out.println(cell.toString());
        }
      }
    });*/
}

}

