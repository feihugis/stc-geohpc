package edu.gmu.stc.climatespark.rdd

import edu.gmu.stc.climatespark.functions.DataFormatFunctions
import edu.gmu.stc.hadoop.raster.{DataChunkCorner, DataChunkHosts, DataChunkCoord}

/**
  * Created by Fei Hu on 1/30/17.
  */
object DataChunkScheduler {

  def equalAssign(dataChunkCoordResults: Array[AnyRef]): Array[(DataChunkHosts, Array[Array[DataChunkCorner]])]  = {
    val scheduledDataChunks = dataChunkCoordResults.groupBy(dataChunkCoord => {
      val coord = dataChunkCoord.asInstanceOf[DataChunkCoord]
      coord.getTime + " , " + coord.getHosts.mkString(",")
    }).values.map(chunkCoords => {
      val key = new DataChunkHosts(chunkCoords(0).asInstanceOf[DataChunkCoord].getHosts)
      val chunkCorners = new Array[DataChunkCorner](chunkCoords.size)
      for ( i <- 0 until chunkCoords.size) {
        chunkCorners(i) = new DataChunkCorner(chunkCoords(i).asInstanceOf[DataChunkCoord].getCorner)
      }
      (key, chunkCorners)
    }).flatMap(tuple => {
      val results = new Array[(String, Array[DataChunkCorner])](tuple._1.getHosts.length)
      val dataChunkSize = tuple._2.length
      val unit = dataChunkSize/results.length
      for (i <- results.indices) {
        val key = tuple._1.getHosts()(i)
        if (unit*(i+2) > dataChunkSize) {
          val value = tuple._2.slice(unit*i, dataChunkSize)
          results(i) = (key, value)
        } else {
          val value = tuple._2.slice(unit*i, unit*(i+1))
          results(i) = (key, value)
        }
      }
      results
    }).groupBy(tuple => {
      tuple._1
    }).values.map(itor => {
      val group = itor.toArray
      val values = new Array[Array[DataChunkCorner]](group.length)
      for (i <- 0 until values.length) {
        values(i) = group(i)._2
      }
      val key = new DataChunkHosts(Array(group(0)._1))
      (key, values.toArray)
    }).toArray

    scheduledDataChunks
  }

  def randomAssign(dataChunkCoordResults: Array[AnyRef]): Array[(DataChunkHosts, Array[Array[DataChunkCorner]])]  = {
    val scheduledDataChunks = dataChunkCoordResults.groupBy(dataChunkCoord => {
      val coord = dataChunkCoord.asInstanceOf[DataChunkCoord]
      coord.getTime + " , " + coord.getHosts.mkString(",")
    }).values.map(chunkCoords => {
      val key = new DataChunkHosts(chunkCoords(0).asInstanceOf[DataChunkCoord].getHosts)
      val chunkCorners = new Array[DataChunkCorner](chunkCoords.size)
      for ( i <- 0 until chunkCoords.size) {
        chunkCorners(i) = new DataChunkCorner(chunkCoords(i).asInstanceOf[DataChunkCoord].getCorner)
      }

      val value = new Array[Array[DataChunkCorner]](1)
      value(0) = chunkCorners
      (key, value)
    }).groupBy(tuple => tuple._1.getHosts.mkString(""))
      .values.map(itor => {
      val group = itor.toArray
      val values = new Array[Array[DataChunkCorner]](group.length)
      for (i <- values.indices) {
        values(i) = group(i)._2(0)
      }
      val key = group(0)._1
      (key, values)
    }).toArray

    scheduledDataChunks
  }

}
