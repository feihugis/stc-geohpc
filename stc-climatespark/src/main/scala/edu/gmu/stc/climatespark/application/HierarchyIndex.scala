package edu.gmu.stc.climatespark.application

import java.util

import edu.gmu.stc.climatespark.core.ClimateSparkContext
import edu.gmu.stc.climatespark.functions.DataFormatFunctions
import edu.gmu.stc.climatespark.rdd.{DataChunkScheduler, DataChunkRDD, DataChunkSplitRDD}
import edu.gmu.stc.hadoop.index.kdtree.KDTree
import edu.gmu.stc.hadoop.raster.{DataChunkCorner, DataChunkHosts, DataChunkCoord, DataChunkMeta}
import org.apache.hadoop.conf.Configuration
import edu.gmu.stc.climatespark.util.RuntimeMonitor

import edu.gmu.stc.hadoop.raster._
import edu.gmu.stc.climatespark.functions.ClimateRDDFunctions._
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.log4j.{Level, Logger}

import edu.gmu.stc.hadoop.commons.ClimateHadoopConfigParameter
import edu.gmu.stc.climatespark.io.datastructure._
import edu.gmu.stc.climatespark.core.ClimateSparkContext
import edu.gmu.stc.climatespark.functions.ClimateRDDFunctions._


/**
  * Created by Fei Hu on 1/30/17.
  */
object HierarchyIndex {


  def qeueryDataChunkRDD(sc: ClimateSparkContext, dataChunkMeta: DataChunkMeta,
                         kdTree: KDTree[DataChunkCoord], start_point: Array[Double], end_point: Array[Double] ): DataChunkRDD = {

    val dataChunkCoordResults = kdTree.range(start_point, end_point)

    val scheduledDataChunks = DataChunkScheduler.randomAssign(dataChunkCoordResults.toArray())

    val dataChunkInputSplitRDD = new DataChunkSplitRDD(scheduledDataChunks, dataChunkMeta, sc.getSparkContext, sc.getHadoopConfig)
    //dataChunkInputSplitRDD.cache()

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

  def qeueryDataChunkRDD(sc: ClimateSparkContext, dataChunkMeta: DataChunkMeta,
                         kdTree: KDTree[DataChunkCoord], boundaryBoxes: Array[(Array[Double], Array[Double])] ): DataChunkRDD = {



    val dataChunkCoordResults = new util.ArrayList[DataChunkCoord]()

    for (boundary <- boundaryBoxes) {
      dataChunkCoordResults.addAll(kdTree.range(boundary._1, boundary._2))
    }

    val scheduledDataChunks = DataChunkScheduler.randomAssign(dataChunkCoordResults.toArray())

    val dataChunkInputSplitRDD = new DataChunkSplitRDD(scheduledDataChunks, dataChunkMeta, sc.getSparkContext, sc.getHadoopConfig)
    //dataChunkInputSplitRDD.cache()

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



  def main(args: Array[String]) {
    val sc = new SparkContext()
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

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
    val end_point = Array(19901231, 1.0, 183.0, 433.0)
    val dataChunkRDD = qeueryDataChunkRDD(csc, dataChunkMeta, kdTree, start_point, end_point)


    RuntimeMonitor.show_multi_runtiming(dataChunkRDD.timeAvg(1).foreach(s => println(s)), 10)


    val years = Array(19860101, 19870101, 19880101, 19890101, 19900101, 19910101, 19920101, 19930101, 19940101, 19950101)
    for (year <- years) {
      val start_point = Array(19850101, 0.0, 254.0, 113.0)
      val end_point = Array(year, 24.0, 262.0, 124.0)
      val dataChunkRDD = qeueryDataChunkRDD(csc, dataChunkMeta, kdTree, start_point, end_point)
      RuntimeMonitor.show_multi_runtiming(dataChunkRDD.avgDaily.foreach(s => println(s)), 10)
    }





  }

}
