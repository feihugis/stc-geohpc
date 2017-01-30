package edu.gmu.stc.climatespark.application

import edu.gmu.stc.climatespark.core.ClimateSparkContext
import edu.gmu.stc.climatespark.functions.DataFormatFunctions
import edu.gmu.stc.climatespark.rdd.{DataChunkRDD, DataChunkSplitRDD}
import edu.gmu.stc.climatespark.util.RuntimeMonitor
import edu.gmu.stc.hadoop.raster._
import edu.gmu.stc.climatespark.functions.ClimateRDDFunctions._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


/**
  * Created by Fei Hu on 12/26/16.
  */
object IndexedRDDTest {

  def main(args: Array[String]) {
    val configFile = "/Users/feihu/Documents/GitHub/stc-geohpc/stc-spark/src/main/resources/merra2-climatespark-config.xml"
    val sc = new ClimateSparkContext(configFile, "local[6]", "test")
    //val sparkConf = new SparkConf().setMaster("local[6]").setAppName("test").set("spark.driver.allowMultipleContexts", "true")

    val filterMask = 0
    val dataType = "float"
    val varName = "PRECCU"
    val dataChunkMeta = new DataChunkMeta(Array(1, 1, 91, 144),
      Array("date", "hour", "lat", "lon"),
      filterMask, dataType, varName,
      "/merra2/daily/M2T1NXINT/YEAR/MONTH/MERRA2_CODE.tavg1_2d_int_Nx.TIME.nc4",
      //"/Users/feihu/Documents/Data/Merra2/MERRA2_200.tavg1_2d_int_Nx.YEAR.nc4",
      "/Users/feihu/Documents/Data/Merra2/index/nodeindex/preccu_HOSTID.txt")
    val filePath = dataChunkMeta.getFilePath(19950101)

    println(filePath)

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

    println(tree_height)

    var time = 0L
    for (i <- 0 until 30) {
      time = time + RuntimeMonitor.show_timing(kdTree.range(Array(19860101, 0.0, 0.0, 0.0), Array(19870101, 24.0, 364.0, 576.0)).toArray)._2
    }

    println(time/30)

    time = 0L
    for (i <- 0 until 30) {
      time = time + RuntimeMonitor.show_timing(kdTree.range(Array(19860101, 0.0, 0.0, 0.0), Array(19880101, 24.0, 364.0, 576.0)).toArray)._2
    }

    println(time/30)

    time = 0L
    for (i <- 0 until 30) {
      time = time + RuntimeMonitor.show_timing(kdTree.range(Array(19860101, 0.0, 0.0, 0.0), Array(19890101, 24.0, 364.0, 576.0)).toArray)._2
    }

    println(time/30)


    time = 0L
    for (i <- 0 until 30) {
      time = time + RuntimeMonitor.show_timing(kdTree.range(Array(19860101, 0.0, 0.0, 0.0), Array(19900101, 24.0, 364.0, 576.0)).toArray)._2
    }

    println(time/30)


    time = 0L
    for (i <- 0 until 30) {
      time = time + RuntimeMonitor.show_timing(kdTree.range(Array(19860101, 0.0, 0.0, 0.0), Array(19910101, 24.0, 364.0, 576.0)).toArray)._2
    }

    println(time/30)


    time = 0L
    for (i <- 0 until 30) {
      time = time + RuntimeMonitor.show_timing(kdTree.range(Array(19860101, 0.0, 0.0, 0.0), Array(19920101, 24.0, 364.0, 576.0)).toArray)._2
    }

    println(time/30)

    time = 0L
    for (i <- 0 until 30) {
      time = time + RuntimeMonitor.show_timing(kdTree.range(Array(19860101, 0.0, 0.0, 0.0), Array(19930101, 24.0, 364.0, 576.0)).toArray)._2
    }

    println(time/30)


    time = 0L
    for (i <- 0 until 30) {
      time = time + RuntimeMonitor.show_timing(kdTree.range(Array(19860101, 0.0, 0.0, 0.0), Array(19940101, 24.0, 364.0, 576.0)).toArray)._2
    }

    println(time/30)


    time = 0L
    for (i <- 0 until 30) {
      time = time + RuntimeMonitor.show_timing(kdTree.range(Array(19860101, 0.0, 0.0, 0.0), Array(19950101, 24.0, 364.0, 576.0)).toArray)._2
    }

    println(time/30)

    time = 0L
    for (i <- 0 until 30) {
      time = time + RuntimeMonitor.show_timing(kdTree.range(Array(19860101, 0.0, 0.0, 0.0), Array(19960101, 24.0, 364.0, 576.0)).toArray)._2
    }

    println(time/30)

    /*
    val dataChunkCoordResults = kdTree.range(Array(19920101, 0.0, 0.0, 0.0), Array(19920102, 24.0, 364.0, 576.0)).toArray

    println("**************************")



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
    /*val paritions = dataChunkRDD.partitions
    val size = dataChunkRDD.count()
    val dependency = dataChunkRDD.dependencies
    println("--------" + size)*/
    dataChunkRDD.avgDaily.collect().foreach(s => println(s))
    */
  }
}
