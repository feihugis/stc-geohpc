package edu.gmu.stc.climatespark.application

import java.util

import edu.gmu.stc.climatespark.core.ClimateSparkContext
import edu.gmu.stc.climatespark.functions.DataFormatFunctions
import edu.gmu.stc.climatespark.rdd.{DataChunkScheduler, DataChunkRDD, DataChunkSplitRDD}
import edu.gmu.stc.hadoop.index.kdtree.KDTree
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer
import edu.gmu.stc.hadoop.raster.{DataChunkCorner, DataChunkHosts, DataChunkCoord, DataChunkMeta}
import org.apache.hadoop.conf.Configuration
import edu.gmu.stc.climatespark.util.RuntimeMonitor

import edu.gmu.stc.hadoop.raster._
import edu.gmu.stc.climatespark.functions.ClimateRDDFunctions._
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.log4j.{Level, Logger}

import edu.gmu.stc.hadoop.commons.ClimateHadoopConfigParameter
import edu.gmu.stc.climatespark.io.datastructure._
import edu.gmu.stc.climatespark.core.ClimateSparkContext
import edu.gmu.stc.climatespark.functions.ClimateRDDFunctions._

import edu.gmu.stc.climatespark.rdd.CustomizedRDDFactory._


/**
  * Created by Fei Hu on 1/30/17.
  */
object HierarchyIndex {


  def main(args: Array[String]) {
    val sc = new SparkContext()
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.INFO)

    val configFile = "/var/lib/hadoop-hdfs/0130/merra2-climatespark-config.xml"
    val csc = new ClimateSparkContext(sc, configFile)

    //val climateRDD = csc.getClimateRDD
    //RuntimeMonitor.show_multi_runtiming(climateRDD.avgDaily.foreach(s => println(s)), 1)

    val filterMask = 0
    val dataType = "float"
    val varName = "PRECCU"
    val dataChunkMeta = new DataChunkMeta(Array(1, 1, 91, 144),
      Array("date", "hour", "lat", "lon"),
      filterMask, dataType, varName,
      "/merra2/daily/M2T1NXINT/YEAR/MONTH/MERRA2_CODE.tavg1_2d_int_Nx.TIME.nc4",
      "/merra2/index/datanode/preccu_HOSTID.txt")

    val kdTreeFile = "/merra2/index/kdindexGson-1.txt"


    val conf = csc.getHadoopConfig
    val kdTree = DataFormatFunctions.kdTreeFileToKDTree(sc, conf, kdTreeFile)


    val start_point = Array(19860101, 0.0, 0.0, 0.0)
    val end_point = Array(19901231, 24.0, 364.0, 576.0)

    val dataChunkRDDs = qeueryDataChunkRDD(csc, dataChunkMeta, kdTree, start_point, end_point, true)
    //701184

    val dynamicDataChunkRDD = queryDynamicDataChunkRDD(csc, dataChunkMeta, kdTree, start_point, end_point, true, 1, 4)
    RuntimeMonitor.show_multi_runtiming(dynamicDataChunkRDD.avgDaily.foreach(p => p), 5)

    RuntimeMonitor.show_multi_runtiming(dataChunkRDDs.avgDaily.foreach(p => p), 5)





    val indexedDataChunkRDD = qeueryIndexedDataChunkRDD(csc, dataChunkMeta, kdTree, start_point, end_point, false)

    indexedDataChunkRDD.cache()
    indexedDataChunkRDD.range(start_point, start_point)

    val indexTime = RuntimeMonitor.show_multi_runtiming(indexedDataChunkRDD.range(start_point, start_point), 10)
    println(indexTime)

    RuntimeMonitor.show_multi_runtiming(
      indexedDataChunkRDD.map(kdTree => {
        kdTree.search(Array(19860101, 0.0, 0.0, 0.0))
      }).filter(p => p != null).foreach(p => p),
      5)

    RuntimeMonitor.show_multi_runtiming(
      indexedDataChunkRDD.map(kdTree => {
        kdTree.range(Array(19860101, 0.0, 0.0, 0.0), Array(19860131, 3.0, 0.0, 0.0))
      }).filter(p => p != null).foreach(p => p),
      5)

    RuntimeMonitor.show_multi_runtiming(
      indexedDataChunkRDD.map(kdTree => {
        kdTree.nearest(Array(19860101, 0.0, 0.0, 0.0))
      }).filter(p => p != null).foreach(p => p),
      5)

    indexedDataChunkRDD.map(kdTree => kdTree.getTreeHeight).collect().foreach(h => println(h))



    RuntimeMonitor.show_multi_runtiming(
      indexedDataChunkRDD.map(kdTree => {
        (2, kdTree.nearest(Array(19860101, 0.0, 0.0, 0.0)))
      }).sortByKey().foreach(p => p),
      5)


    //RuntimeMonitor.show_multi_runtiming(indexedDataChunkRDD.map(kdTree => kdTree.range(Array(19860101, 0.0, 0.0, 0.0), Array(19860101, 24.0, 183.0, 443.0))), 5)

    val dataChunkRDD = qeueryDataChunkRDD(csc, dataChunkMeta, kdTree, start_point, end_point, false)
    dataChunkRDD.cache()
    dataChunkRDD.filter(tuple => {
      val chunkCornerID = new DataChunkCorner(tuple._1).getID
      if (chunkCornerID == 1.9920101000000000E16) {
        true
      } else {
        false
      }
    }).collect()

    val nonIndexTime = RuntimeMonitor.show_multi_runtiming(
      dataChunkRDD.map(tuple => {
        val chunkCorner = new DataChunkCorner(tuple._1).getCorner
        val corner = Array(19860101, 0, 0, 0)
        var isEqual = true
        for (i <- chunkCorner.indices) {
          if (chunkCorner(i) != corner(i)) {
            isEqual = false
          }
        }
        (isEqual, tuple)
      }).filter(t => t._1).collect(), 5)

    RuntimeMonitor.show_multi_runtiming(
      dataChunkRDD.map(tuple => {
        val chunkCornerID = new DataChunkCorner(tuple._1).getID
        (chunkCornerID, tuple)
      }).filter(t => t._1 == 1.9860101000000000E16).collect().length, 5)

    //println(nonIndexTime)

    RuntimeMonitor.show_multi_runtiming(
      dataChunkRDD.filter(tuple => {
        val corner = new DataChunkCorner(tuple._1).getCorner
        val start = Array(19860101, 0, 0, 0)
        val end = Array(19860131, 3, 0, 0)

        var isIn = true

        for (i <- corner.indices) {
          if (corner(i) < start(i) || corner(i) > end(i)) {
            isIn = false
          }
        }

        isIn
      }).collect(), 5)

    dataChunkRDD.mapPartitions(itor => {
      val result = new Array[(Double, ArraySerializer)](itor.size)
      var i = 0
      while (itor.hasNext) {
        val tuple = itor.next()
        if (tuple != null) {
          val dataChunkCorner = new DataChunkCorner(tuple._1)
          val point = new DataChunkCorner(Array(19860101, 0, 0, 0))

          var dist = 0.0;
          for (i <- dataChunkCorner.getCorner.indices) {
            dist = dist + (dataChunkCorner.getCorner()(i) - point.getCorner()(i)) * (dataChunkCorner.getCorner()(i) - point.getCorner()(i));
          }

          dist = Math.sqrt(dist)
          result(i) = (dist, tuple._2)
          i = i + 1
        }

      }
      result.sortBy(t => t._1)
      result.toIterator
    }).collect()

    RuntimeMonitor.show_multi_runtiming(
      dataChunkRDD.map(tuple => {
      val dataChunkCorner = new DataChunkCorner(tuple._1)
      val point = new DataChunkCorner(Array(19860101, 0, 0, 0))

      var dist = 0.0;
      for (i <- dataChunkCorner.getCorner.indices) {
        dist = dist + (dataChunkCorner.getCorner()(i) - point.getCorner()(i)) * (dataChunkCorner.getCorner()(i) - point.getCorner()(i));
      }
      dist = Math.sqrt(dist)
      (dist.toInt, tuple._2)
    }).mapPartitions(itor => {
        itor.toArray.sortBy(t => t._1).slice(0, 1).toIterator
      }).sortByKey().count(), 5)

    dataChunkRDD.mapPartitions(partition => Array(partition.length).toIterator).collect().foreach(p => println(p))

    val mapRDD =indexedDataChunkRDD.map(kdTree => kdTree.range(Array(19860101, 2.0, 91.0, 144.0), Array(19860101, 2.0, 91.0, 144.0))).filter(result => !result.isEmpty).collect()

    val size = indexedDataChunkRDD.map(kdTree => kdTree.size()).reduce(_ + _)


    //RuntimeMonitor.show_multi_runtiming(dataChunkRDD.timeAvg(1).foreach(s => println(s)), 10)

    val start_point1 = Array(19860101, 0.0, 273.0, 0.0)
    val end_point1 = Array(19901231, 1.0, 273.0, 144.0)
    val boundaryBox = Array((start_point, end_point), (start_point1, end_point1))
    //val dataChunkRDD = qeueryDataChunkRDD(csc, dataChunkMeta, kdTree, boundaryBox, true)
    dataChunkRDD.count()



    dataChunkRDD.cache()

    val queryCornerID = new DataChunkCorner(Array(19860606, 2, 91, 144)).getID

    dataChunkRDD.filter(tuple => {
      val chunkCornerID = new DataChunkCorner(tuple._1).getID
      if (chunkCornerID == 1.9860606002091144E16) {
        true
      } else {
        false
      }
    }).collect().size


    RuntimeMonitor.show_multi_runtiming(dataChunkRDD.timeAvg(1).foreach(s => println(s)), 5)


    RuntimeMonitor.show_multi_runtiming(dataChunkRDD.foreach(p => println(p)), 2)




    val years = Array(19860101, 19870101, 19880101, 19890101, 19900101, 19910101, 19920101, 19930101, 19940101, 19950101)
    for (year <- years) {
      val start_point = Array(19850101, 0.0, 254.0, 113.0)
      val end_point = Array(year, 24.0, 262.0, 124.0)
      val dataChunkRDD = qeueryDataChunkRDD(csc, dataChunkMeta, kdTree, start_point, end_point, true)
      RuntimeMonitor.show_multi_runtiming(dataChunkRDD.avgDaily.foreach(s => println(s)), 10)

    }
  }

}
