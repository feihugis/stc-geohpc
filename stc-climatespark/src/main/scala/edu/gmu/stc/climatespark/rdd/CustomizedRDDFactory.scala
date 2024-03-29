package edu.gmu.stc.climatespark.rdd

import java.util

import scala.math._
import edu.gmu.stc.climatespark.core.ClimateSparkContext
import edu.gmu.stc.hadoop.index.kdtree.KDTree
import edu.gmu.stc.hadoop.raster.{DataChunkCoord, DataChunkCorner, DataChunkHosts, DataChunkMeta}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Fei Hu on 2/9/17.
  */
object CustomizedRDDFactory {

  def qeueryDataChunkRDD(sc: ClimateSparkContext, dataChunkMeta: DataChunkMeta,
                         kdTree: KDTree[DataChunkCoord], start_point: Array[Double], end_point: Array[Double], isCache: Boolean ): DataChunkRDD = {

    val dataChunkCoordResults = kdTree.range(start_point, end_point)

    val scheduledDataChunks = DataChunkScheduler.randomAssign(dataChunkCoordResults.toArray())

    val dataChunkInputSplitRDD = new DataChunkSplitRDD(scheduledDataChunks, dataChunkMeta, sc.getSparkContext, sc.getHadoopConfig)

    if (isCache) {
      dataChunkInputSplitRDD.cache()
    }

    var inputSplitSize = 0

    val oneToMorePatitionNumMapping = new Array[(Int, Int)](scheduledDataChunks.length)
    for (i <- oneToMorePatitionNumMapping.indices) {
      val size = scheduledDataChunks(i)._2.length
      if (i > 0) {
        oneToMorePatitionNumMapping(i) = (i, size + oneToMorePatitionNumMapping(i-1)._2)
      } else {
        oneToMorePatitionNumMapping(i) = (i, size)
      }
      inputSplitSize = inputSplitSize + size
    }

    val dataChunkRDD = new DataChunkRDD(dataChunkInputSplitRDD, oneToMorePatitionNumMapping, inputSplitSize, sc.getSparkContext, sc.getHadoopConfig)
    dataChunkRDD
  }

  def queryDynamicDataChunkRDD(sc: ClimateSparkContext, dataChunkMeta: DataChunkMeta,
                               kdTree: KDTree[DataChunkCoord],
                               start_point: Array[Double], end_point: Array[Double], isCache: Boolean,
                               increaseParallelismLevel: Int = 1, decreaseParallelismLevel: Int = 1): DataChunkRDD = {

    val dataChunkCoordResults = kdTree.range(start_point, end_point)

    var scheduledDataChunks = DataChunkScheduler.randomAssign(dataChunkCoordResults.toArray())

    if ( increaseParallelismLevel > 1) {
      scheduledDataChunks = scheduledDataChunks.map(tuple => {
        val splitNum = increaseParallelismLevel
        var newAssign = ArrayBuffer.empty[Array[DataChunkCorner]]
        for (dataChunkCornerArray <- tuple._2) {
          for(one <- dataChunkCornerArray.grouped(dataChunkCornerArray.length/splitNum)) {
            newAssign += one
          }
        }
        (tuple._1, newAssign.toArray)
      })
    }

    val dataChunkInputSplitRDD = new DataChunkSplitRDD(scheduledDataChunks, dataChunkMeta, sc.getSparkContext, sc.getHadoopConfig)

    if (isCache) {
      dataChunkInputSplitRDD.cache()
    }

    var inputSplitSize = 0

    val oneToMorePatitionNumMapping = new Array[(Int, Int)](scheduledDataChunks.length)
    for (i <- oneToMorePatitionNumMapping.indices) {
      val size = scheduledDataChunks(i)._2.length
      if (i > 0) {
        oneToMorePatitionNumMapping(i) = (i, size + oneToMorePatitionNumMapping(i-1)._2)
      } else {
        oneToMorePatitionNumMapping(i) = (i, size)
      }
      inputSplitSize = inputSplitSize + (0 until size by decreaseParallelismLevel).length
    }

    //inputSplitSize = max((0 until inputSplitSize by decreaseParallelismLevel).length, scheduledDataChunks.length)

    val dataChunkRDD = new DataChunkRDD(dataChunkInputSplitRDD, oneToMorePatitionNumMapping,
      inputSplitSize, sc.getSparkContext, sc.getHadoopConfig, decreaseParallelismLevel)

    dataChunkRDD
  }

  def qeueryDataChunkRDD(sc: ClimateSparkContext, dataChunkMeta: DataChunkMeta,
                         kdTree: KDTree[DataChunkCoord], boundaryBoxes: Array[(Array[Double], Array[Double])], isCache: Boolean ): DataChunkRDD = {
    val dataChunkCoordResults = new util.ArrayList[DataChunkCoord]()

    for (boundary <- boundaryBoxes) {
      dataChunkCoordResults.addAll(kdTree.range(boundary._1, boundary._2))
    }

    val scheduledDataChunks = DataChunkScheduler.randomAssign(dataChunkCoordResults.toArray())

    val dataChunkInputSplitRDD = new DataChunkSplitRDD(scheduledDataChunks, dataChunkMeta, sc.getSparkContext, sc.getHadoopConfig)

    if (isCache) {
      dataChunkInputSplitRDD.cache()
    }

    var inputSplitSize = 0

    val oneToMorePatitionNumMapping = new Array[(Int, Int)](scheduledDataChunks.length)
    for (i <- oneToMorePatitionNumMapping.indices) {
      val size = scheduledDataChunks(i)._2.length
      if (i > 0) {
        oneToMorePatitionNumMapping(i) = (i, size + oneToMorePatitionNumMapping(i-1)._2)
      } else {
        oneToMorePatitionNumMapping(i) = (i, size)
      }
      inputSplitSize = inputSplitSize + size
    }

    val dataChunkRDD = new DataChunkRDD(dataChunkInputSplitRDD, oneToMorePatitionNumMapping, inputSplitSize, sc.getSparkContext, sc.getHadoopConfig)
    dataChunkRDD
  }


  def qeueryIndexedDataChunkRDD(sc: ClimateSparkContext, dataChunkMeta: DataChunkMeta,
                                kdTree: KDTree[DataChunkCoord], start_point: Array[Double], end_point: Array[Double], isCache: Boolean ): IndexedDataChunkRDD = {
    val dataChunkCoordResults = kdTree.range(start_point, end_point).toArray()

    val scheduledDataChunks = DataChunkScheduler.randomAssign(dataChunkCoordResults)

    var inputSplitSize = 0

    val oneToMorePatitionNumMapping = new Array[(Int, Int)](scheduledDataChunks.length)
    for (i <- oneToMorePatitionNumMapping.indices) {
      val size = scheduledDataChunks(i)._2.length
      if (i > 0) {
        oneToMorePatitionNumMapping(i) = (i, size + oneToMorePatitionNumMapping(i-1)._2)
      } else {
        oneToMorePatitionNumMapping(i) = (i, size)
      }
      inputSplitSize = inputSplitSize + size
    }

    val dataChunkInputSplitRDD = new DataChunkSplitRDD(scheduledDataChunks, dataChunkMeta, sc.getSparkContext, sc.getHadoopConfig)

    if (isCache) {
      dataChunkInputSplitRDD.cache()
    }

    val dataChunkRDD = new IndexedDataChunkRDD(dataChunkInputSplitRDD, oneToMorePatitionNumMapping, inputSplitSize, sc.getSparkContext, sc.getHadoopConfig)
    dataChunkRDD
  }
}
