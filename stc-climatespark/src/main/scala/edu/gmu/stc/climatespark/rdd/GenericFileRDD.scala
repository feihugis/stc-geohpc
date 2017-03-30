package edu.gmu.stc.climatespark.rdd

import edu.gmu.stc.hadoop.raster.{DataChunk, DataChunkInputFormat, GenericFileInputFormat}
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.NewHadoopRDD

/**
  * Created by Fei Hu on 3/29/17.
  */
class GenericFileRDD (sc : SparkContext,
                      @transient conf: Configuration) extends NewHadoopRDD(sc,
  classOf[GenericFileInputFormat]
    .asInstanceOf[Class[F] forSome {type F <: InputFormat[Text, Text]}],
  classOf[org.apache.hadoop.io.Text],
  classOf[org.apache.hadoop.io.Text],
  conf){

}