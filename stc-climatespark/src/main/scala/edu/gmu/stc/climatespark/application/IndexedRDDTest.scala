package edu.gmu.stc.climatespark.application

import java.util

import edu.gmu.stc.climatespark.core.ClimateSparkContext
import edu.gmu.stc.climatespark.functions.DataFormatFunctions
import edu.gmu.stc.climatespark.rdd.{IndexedDataChunkRDD, DataChunkScheduler, DataChunkRDD, DataChunkSplitRDD}
import edu.gmu.stc.climatespark.util.RuntimeMonitor
import edu.gmu.stc.hadoop.index.kdtree.KDTree
import edu.gmu.stc.hadoop.raster._
import edu.gmu.stc.climatespark.functions.ClimateRDDFunctions._
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import edu.gmu.stc.climatespark.rdd.CustomizedRDDFactory._

import edu.gmu.stc.climatespark.functions.ClimateRDDFunctions._

/**
  * Created by Fei Hu on 12/26/16.
  */
object IndexedRDDTest {

  def main(args: Array[String]) {
    val configFile = "/Users/feihu/Documents/GitHub/stc-geohpc/stc-spark/src/main/resources/merra2-climatespark-config.xml"
    val sc = new ClimateSparkContext(configFile, "local[6]", "test")
    val sqlContext = new SQLContext(sc.getSparkContext)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    import sqlContext.implicits._

    /*val climateRDD = sc.getClimateRDD
    val cellRDD = climateRDD.query3DPointTimeSeries
    val df = cellRDD.toDF()
    df.show()
    df.saveAsParquetFile("/Users/feihu/Desktop/preccu.parquet")*/

    //val sparkConf = new SparkConf().setMaster("local[6]").setAppName("test").set("spark.driver.allowMultipleContexts", "true")
    //   "/Users/feihu/Documents/Data/Merra2/MERRA2_CODE.tavg1_2d_int_Nx.TIME.nc4",
    //   "/Users/feihu/Documents/Data/Merra2/index/nodeindex/preccu_HOSTID.txt"
    val filterMask = 0
    val dataType = "float"
    val varName = "PRECCU"
    val dataChunkMeta = new DataChunkMeta(Array(1, 1, 91, 144),
      Array("date", "hour", "lat", "lon"),
      filterMask, dataType, varName,
      "/Users/feihu/Documents/Data/Merra2/MERRA2_CODE.tavg1_2d_int_Nx.TIME.nc4",
      "/Users/feihu/Documents/Data/Merra2/index/nodeindex/preccu_HOSTID.txt")


    val inputFile = "/Users/feihu/Documents/Data/Merra2/index/preccu"
    val outputFile = "/Users/feihu/Documents/Data/Merra2/index/preccu-index-test.parquet"
    val kdTreeFile = "/Users/feihu/Documents/Data/Merra2/index/kdindexGson-1.txt"
    //DataFormatFunctions.csvToParquet(inputFile, outputFile, 100, sc.getSparkContext)
    //val dataChunkRDD = DataFormatFunctions.parquetToDataChunkRDD(sc.getSparkContext, outputFile)
    //val dataChunkCoordRDD = DataFormatFunctions.parquetToDataChunkCoordRDD(sc.getSparkContext, outputFile)
    //DataFormatFunctions.csvToDataNodeIndexFile(inputFile, "/Users/feihu/Documents/Data/Merra2/index/nodeindex/", "preccu", 23, sc.getSparkContext)
    //DataFormatFunctions.csvToKDTreeFile(sc.getSparkContext, sc.getHadoopConfig, inputFile, kdTreeFile)


    val kdTree = DataFormatFunctions.kdTreeFileToKDTree(sc.getSparkContext, sc.getHadoopConfig, kdTreeFile)
    //376 113
    val tree_height = kdTree.getTreeHeight

    //println(tree_height)

    val repeatTime = 30

    //println(RuntimeMonitor.show_multi_runtiming(kdTree.range(Array(19860101, 0.0, 0.0, 0.0), Array(19870101, 24.0, 364.0, 576.0)).toArray, repeatTime))

    val start_point = Array(19860101, 0.0, 0.0, 0.0)
    val end_point = Array(19900102, 24.0, 364.0, 576.0)

    val results = kdTree.range(start_point, end_point)

    val start_point2 = Array(19920101, 15.0, 0.0, 0.0)
    val end_point2 = Array(19920102, 24.0, 10.0, 43.0)

    val boundaryBox = new Array[(Array[Double], Array[Double])](2)
    boundaryBox(0) = (start_point, end_point)
    boundaryBox(1) = (start_point2, end_point2)

    val dataChunkRDD = qeueryDataChunkRDD(sc, dataChunkMeta, kdTree, start_point, end_point, false)
    dataChunkRDD.cache()

    val dynamicDataChunkRDD = queryDynamicDataChunkRDD(sc, dataChunkMeta,
                                                       kdTree, start_point, end_point, false,
                                                       1, 2)

    dynamicDataChunkRDD.count()



    val indexedDataChunkRDD = qeueryIndexedDataChunkRDD(sc, dataChunkMeta, kdTree, start_point, end_point, false)
    indexedDataChunkRDD.cache()


    indexedDataChunkRDD.range(start_point, start_point)

    val result = indexedDataChunkRDD.nearestEuclidean(start_point, 1.0)

   // indexedDataChunkRDD.map(kdTree => kdTree.nearestEuclidean(start_point, 1.0)).collect().size

    val indexTime = RuntimeMonitor.show_multi_runtiming(indexedDataChunkRDD.range(start_point, start_point), 10)
    println(indexTime)

    dataChunkRDD.filter(tuple => {
      val chunkCornerID = new DataChunkCorner(tuple._1).getID
      if (chunkCornerID == 1.9920101000000000E16) {
        true
      } else {
        false
      }
    }).collect()

    val nonIndexTime = RuntimeMonitor.show_multi_runtiming(
      dataChunkRDD.filter(tuple => {
        val chunkCornerID = new DataChunkCorner(tuple._1).getID
        if (chunkCornerID == 1.9920101000000000E16) {
          true
        } else {
          false
        }
      }).collect(), 10)

    println(nonIndexTime)

    println("------------")
    RuntimeMonitor.show_multi_runtiming(
      indexedDataChunkRDD.map(kdTree => {
        kdTree.search(Array(19920101, 0.0, 0.0, 0.0))
      }).filter(p => p != null).foreach(p => p),
      5)

    RuntimeMonitor.show_multi_runtiming(
      indexedDataChunkRDD.map(kdTree => {
        kdTree.range(Array(19920101, 0.0, 0.0, 0.0), Array(19920131, 3.0, 0.0, 0.0))
      }).filter(p => p != null).foreach(p => p),
      5)

    RuntimeMonitor.show_multi_runtiming(
      indexedDataChunkRDD.map(kdTree => {
        kdTree.nearest(Array(19920101, 0.0, 0.0, 0.0))
      }).filter(p => p != null).foreach(p => p),
      5)

    RuntimeMonitor.show_multi_runtiming(
      dataChunkRDD.map(tuple => {
        val chunkCorner = new DataChunkCorner(tuple._1).getCorner
        val corner = Array(19920101, 0, 0, 0)
        var isEqual = true
        for (i <- chunkCorner.indices) {
          if (chunkCorner(i) != corner(i)) {
            isEqual = false
          }
        }
        (isEqual, tuple)
      }).filter(t => t._1).collect(), 10)

    RuntimeMonitor.show_multi_runtiming(
      dataChunkRDD.filter(tuple => {
        val chunkCornerID = new DataChunkCorner(tuple._1).getID
        if (chunkCornerID == 1.9920101000000000E16) {
          true
        } else {
          false
        }
      }).collect(), 10)

    RuntimeMonitor.show_multi_runtiming(
      dataChunkRDD.filter(tuple => {
        val corner = new DataChunkCorner(tuple._1).getCorner
        val start = Array(19920101, 0, 0, 0)
        val end = Array(19920131, 3, 0, 0)

        var isIn = true

        for (i <- corner.indices) {
          if (corner(i) < start(i) || corner(i) > end(i)) {
            isIn = false
          }
        }

        isIn
      }).collect(), 10)

    RuntimeMonitor.show_multi_runtiming(
      dataChunkRDD.map(tuple => {
        val dataChunkCorner = new DataChunkCorner(tuple._1)
        val point = new DataChunkCorner(Array(19920101, 0, 0, 0))

        var dist = 0.0;
        for (i <- dataChunkCorner.getCorner.indices) {
          dist = dist + (dataChunkCorner.getCorner()(i) - point.getCorner()(i)) * (dataChunkCorner.getCorner()(i) - point.getCorner()(i));
        }
        dist = Math.sqrt(dist)
        (dist.toInt, tuple._2)
      }).mapPartitions(itor => {
        itor.toArray.sortBy(t => t._1).slice(0, 1).toIterator
      }).sortByKey().count(), 10)

    println("------------------------")


    val mapRDD =indexedDataChunkRDD.map(kdTree => kdTree.range(start_point, end_point)).collect()

    indexedDataChunkRDD.collect().map(kdTree => println(kdTree.getTreeHeight))

    //println(dataChunkRDD.count())
    //println(indexedDataChunkRDD.count())

    val queriedDataChunkCoordResults = kdTree.range(start_point, end_point).toArray()
    val schedulDataChunks = DataChunkScheduler.randomAssign(queriedDataChunkCoordResults)

    //RuntimeMonitor.show_timing(dataChunkRDD.avgDaily.foreach(p => p))
    //RuntimeMonitor.show_timing(dataChunkRDD.avgDaily.foreach(p => p))
    /*val dataChunkCoordResults = kdTree.range(Array(19920101, 0.0, 0.0, 0.0), Array(19920102, 24.0, 364.0, 576.0)).toArray()

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
    dataChunkInputSplitRDD.cache()
    val dataChunkRDD = new DataChunkRDD(dataChunkInputSplitRDD, oneToMorePatitionNumMapping, inputSplitSize, sc.getSparkContext, sc.getHadoopConfig)
    dataChunkRDD.avgDaily.collect().foreach(s => println(s))*/

    /*val paritions = dataChunkRDD.partitions
   val size = dataChunkRDD.count()
   val dependency = dataChunkRDD.dependencies
   println("--------" + size)*/

  }
}
