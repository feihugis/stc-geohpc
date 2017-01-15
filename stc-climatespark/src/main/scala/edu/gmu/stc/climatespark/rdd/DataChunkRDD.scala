package edu.gmu.stc.climatespark.rdd

import edu.gmu.stc.climatespark.rdd.indexedrdd.IndexedRDD
import edu.gmu.stc.climatespark.rdd.indexedrdd.IndexedRDD._
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer
import edu.gmu.stc.hadoop.raster.{DataChunkInputSplit, DataChunk, DataChunkReader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.InputSplit
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import org.apache.spark.storage.StorageLevel



class DataChunkRDD(dataChunkSplitRDD: DataChunkSplitRDD,
                   sc: SparkContext,
                   @transient conf: Configuration)
    extends RDD[(DataChunk, ArraySerializer)](sc, List(new OneToOneDependency(dataChunkSplitRDD))){

  private val confBroadcast = sc.broadcast(new SerializableWritable(conf))

  def getConf: Configuration = {
    val conf: Configuration = confBroadcast.value.value
    conf
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(DataChunk, ArraySerializer)] = {
    val dataChunkSplits = dataChunkSplitRDD.iterator(split, context).toArray
    val results = new ArrayBuffer[(DataChunk, ArraySerializer)]
    val configuration = dataChunkSplitRDD.getConf

    for ( dataChunkSplit <- dataChunkSplits) {
      val recordReader = new DataChunkReader()
      recordReader.initialize(dataChunkSplit, dataChunkSplitRDD.getConf)
      while (recordReader.nextKeyValue()) {
        val key = recordReader.getCurrentKey
        val value = recordReader.getCurrentValue
        if (key != null) {
          val r = (key, value)
          results += r
        }
      }
    }
    results.toIterator
  }

  override protected def getPartitions: Array[Partition] = {
    //partitionsRDD.toLocalIterator.toArray
    //val numPartitions = partitionsRDD.partitions.size
    //partitionsRDD.partitions
    //this.sc.statusTracker.getStageInfo()
    //val partitions = firstParent[DataChunkPartition].partitions
    //partitions
    dataChunkSplitRDD.partitions

  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    dataChunkSplitRDD.preferredLocations(split)
  }

}




