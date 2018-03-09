package edu.gmu.stc.climatespark.rdd

import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer
import edu.gmu.stc.hadoop.raster.{DataChunk, DataChunkInputFormat}
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.NewHadoopRDD

/**
  * Created by Fei Hu on 12/22/16.
  */
class ClimateRDD(
    sc : SparkContext,
    @transient conf: Configuration) extends NewHadoopRDD(sc,
            classOf[DataChunkInputFormat]
                .asInstanceOf[Class[F] forSome {type F <: InputFormat[DataChunk, ArraySerializer]}],
            classOf[edu.gmu.stc.hadoop.raster.DataChunk],
            classOf[edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer],
            conf){

}
