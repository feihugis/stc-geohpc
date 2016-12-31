package edu.gmu.stc.climatespark.rdd

import edu.gmu.stc.hadoop.raster.{DataChunkInputSplit, DataChunk}
import org.apache.spark.{SerializableWritable, Partition}
import scala.collection.JavaConverters._
import org.apache.spark.rdd.ShuffledRDDPartition

/**
  * Created by Fei Hu on 12/28/16.
  */
class DataChunkPartition (val index: Int,
                         @transient dataChunks: Seq[DataChunk]) extends Partition{

  val serializableDataChunk = new SerializableWritable(new DataChunkInputSplit(dataChunks.toList.asJava))

  override def hashCode(): Int = index
}

object DataChunkPartition {
  def apply(index: Int, dataChunks: Seq[DataChunk]): DataChunkPartition = {
    new DataChunkPartition(index, dataChunks)
  }
}
