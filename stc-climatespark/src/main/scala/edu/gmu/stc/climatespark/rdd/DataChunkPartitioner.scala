package edu.gmu.stc.climatespark.rdd

import org.apache.spark.Partitioner

/**
  * Created by Fei Hu on 12/28/16.
  */
class DataChunkPartitioner (partitionNum: Int) extends Partitioner {
  require(partitionNum >= 0, s"Number of partitions ($partitionNum) cannot be negative.")

  def numPartitions: Int = partitionNum

  def getPartition(key: Any): Int = {
    //val rawMod = key.hashCode() % partitionNum
    //rawMod + (if (rawMod < 0) partitionNum else 0)
    val index = key match {
      case Some(x: Long) => x.toInt
      case _ => 0
    }

    index
  }
}
