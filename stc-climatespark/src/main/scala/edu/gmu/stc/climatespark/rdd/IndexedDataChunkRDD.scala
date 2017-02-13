package edu.gmu.stc.climatespark.rdd

import edu.gmu.stc.climatespark.rdd.indexedrdd.IndexedRDDPartition
import edu.gmu.stc.hadoop.index.kdtree.KDTree
import edu.gmu.stc.hadoop.raster.{DataChunkCoord, DataChunkInputSplit, DataChunkReader, DataChunk}
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer
import org.apache.hadoop.conf.Configuration
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{TaskContext, Partition, SerializableWritable, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Fei Hu on 2/1/17.
  */
class IndexedDataChunkRDD(dataChunkSplitRDD: DataChunkSplitRDD,
                          oneToMorePartitionNumMapping: Array[(Int, Int)],
                          newPartitionNum: Int,
                          sc: SparkContext,
                          @transient conf: Configuration)
  extends RDD[KDTree[ArraySerializer]](sc, Nil){   //List(new OneToOneDependency(dataChunkSplitRDD))

  private val confBroadcast = sc.broadcast(new SerializableWritable(conf))

  def getConf: Configuration = {
    val conf: Configuration = confBroadcast.value.value
    conf
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[KDTree[ArraySerializer]] = {
    val parentSplit = getParentPartition(split)
    val inputSplitSplitIndex = {
      if (parentSplit.index == 0) {
        split.index
      } else {
        split.index - oneToMorePartitionNumMapping(parentSplit.index - 1)._2
      }
    }

    val dataChunkSplits = dataChunkSplitRDD.iterator(parentSplit, context).slice(inputSplitSplitIndex, inputSplitSplitIndex+1).toArray
    val results = new ArrayBuffer[(DataChunk, ArraySerializer)]
    val configuration = dataChunkSplitRDD.getConf

    val kdTree = new KDTree[ArraySerializer](4)

    for ( dataChunkSplit <- dataChunkSplits) {
      val recordReader = new DataChunkReader()
      recordReader.initialize(dataChunkSplit, configuration)
      while (recordReader.nextKeyValue()) {
        val key = recordReader.getCurrentKey
        val value = recordReader.getCurrentValue
        if (key != null) {
          val corner = new DataChunkCoord(key).getCornerAsDouble
          kdTree.insert(corner, value)
        }
      }
    }

    Array(kdTree).toIterator
  }

  def range(lowk: Array[Double], uppk: Array[Double]): Array[Array[_ <: AnyRef]] = {
   //this.map(kdTree => kdTree.range(lowk, uppk))
   val results = context.runJob(this, (task: TaskContext, iter: Iterator[KDTree[ArraySerializer]]) => {
      if (iter.hasNext) {
        val kdTree = iter.next()
        //println(kdTree.toString)
        //println("--------------------------------------------------------------------------------")
        kdTree.range(lowk, uppk).toArray()
      } else {
        Array.empty
      }
    }, this.partitions.indices, allowLocal = true)

    results
  }

  def nearestEuclidean(point: Array[Double], dist: Double): Array[Array[_ <: AnyRef]] = {
    val results = context.runJob(this, (task: TaskContext, iter: Iterator[KDTree[ArraySerializer]]) => {
      if (iter.hasNext) {
        val kdTree = iter.next()
        kdTree.nearestEuclidean(point, dist).toArray()
      } else {
        Array.empty
      }
    }, this.partitions.indices, allowLocal = true)

    results
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
    for (i <- parentPartions.indices) {
      val dataChunkScheduler = parentPartions(i).asInstanceOf[DataChunkSplitPartition].dataChunkPartitions
      if (i == 0) {
        startIndex = 0
      } else {
        startIndex = oneToMorePartitionNumMapping(i - 1)._2
      }
      endIndex = oneToMorePartitionNumMapping(i)._2
      for (partitionIndex <- startIndex until endIndex) {
        partitions(partitionIndex) = new DataChunkSplitPartition(dataChunkScheduler, partitionIndex)
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
