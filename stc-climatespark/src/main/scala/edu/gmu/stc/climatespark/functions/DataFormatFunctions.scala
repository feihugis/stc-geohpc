package edu.gmu.stc.climatespark.functions

import edu.gmu.stc.hadoop.raster.DataChunk
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}
import edu.gmu.stc.climatespark.util.{Schema, RuntimeMonitor}
import scala.reflect._

import scala.reflect.ClassTag

/**
  * Created by Fei Hu on 12/28/16.
  */
object DataFormatFunctions {
  def csvToParquet(inputFile : String, outputFile:String, partitionNum: Int, sc : SparkContext) = {
    val sqlContext = new SQLContext(sc)
    val rdd = sc.textFile(inputFile).map(r => new DataChunk(r)).map(atts =>
      (atts.getId, Row(atts.getCorner, atts.getShape, atts.getDimensions, atts.getFilePos,
        atts.getByteSize, atts.getFilterMask, atts.getHosts, atts.getDataType,
        atts.getVarShortName, atts.getFilePath, atts.getTime, atts.getGeometryInfo)))
        .repartition(partitionNum).sortByKey().map(d => d._2)

    val schema = Schema.getDataChunkSchema()
    val rowDF = sqlContext.createDataFrame(rdd, schema)
    rowDF.saveAsParquetFile(outputFile)
  }

  def parquetToRDD(sc : SparkContext, inputFile : String) : RDD[DataChunk] = {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.parquet(inputFile)
    df.map(r => Schema.getDataChunkFromRow(r))
  }
}
