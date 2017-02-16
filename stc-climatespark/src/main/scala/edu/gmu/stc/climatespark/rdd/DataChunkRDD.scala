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
                   oneToMorePartitionNumMapping: Array[(Int, Int)],
                   newPartitionNum: Int,
                   sc: SparkContext,
                   @transient conf: Configuration,
                   decreaseParallelismLevel: Int = 1)
    extends RDD[(DataChunk, ArraySerializer)](sc, Nil){   //List(new OneToOneDependency(dataChunkSplitRDD))

  private val confBroadcast = sc.broadcast(new SerializableWritable(conf))

  def getConf: Configuration = {
    val conf: Configuration = confBroadcast.value.value
    conf
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(DataChunk, ArraySerializer)] = {
    val parentSplit = getParentPartition(split)
    val inputSplitSplitIndex = {
      if (parentSplit.index == 0) {
        split.index
      } else {
        split.index - oneToMorePartitionNumMapping(parentSplit.index - 1)._2
      }
    }

    val splitSize = oneToMorePartitionNumMapping(parentSplit.index)._2

    val dataChunkSplits = dataChunkSplitRDD.iterator(parentSplit, context)
                            .slice(inputSplitSplitIndex,
                                    math.min(inputSplitSplitIndex + decreaseParallelismLevel, splitSize))

    val results = new ArrayBuffer[(DataChunk, ArraySerializer)]
    val configuration = dataChunkSplitRDD.getConf

    for ( dataChunkSplit <- dataChunkSplits) {
      val recordReader = new DataChunkReader()
      recordReader.initialize(dataChunkSplit, configuration)
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

    val parentPartions = dataChunkSplitRDD.partitions
    val partitions = new Array[Partition](newPartitionNum)

    var startIndex = 0
    var endIndex = 0
    var curPartition = 0
    for (i <- parentPartions.indices) {
      val dataChunkScheduler = parentPartions(i).asInstanceOf[DataChunkSplitPartition].dataChunkPartitions
      if (i == 0) {
        startIndex = 0
      } else {
        startIndex = oneToMorePartitionNumMapping(i - 1)._2
      }
      endIndex = oneToMorePartitionNumMapping(i)._2
      for (partitionIndex <- startIndex until endIndex by decreaseParallelismLevel) {
        partitions(curPartition) = new DataChunkSplitPartition(dataChunkScheduler, partitionIndex)
        curPartition = curPartition + 1
      }
    }

    partitions
  }


  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    //val parentSplit = getParentPartition(split)
    //dataChunkSplitRDD.preferredLocations(parentSplit)
    split.asInstanceOf[DataChunkSplitPartition].dataChunkPartitions._1.getHosts
  }

  def getParentPartition(split: Partition): Partition = {
    val index = split.index
    var parentIndex = 0
    var done = false
    for (i <- oneToMorePartitionNumMapping.indices; if !done) {
      if (oneToMorePartitionNumMapping(i)._2 > index) {
        parentIndex = i
        done = true
      }
    }

    new DataChunkSplitPartition(split.asInstanceOf[DataChunkSplitPartition].dataChunkPartitions, parentIndex)
  }
}





