package edu.gmu.stc.climatespark.application

import edu.gmu.stc.climatespark.core.ClimateSparkContext
import edu.gmu.stc.climatespark.functions.DataFormatFunctions
import edu.gmu.stc.climatespark.rdd.{IndexedDataChunkRDD, DataChunkScheduler, DataChunkRDD, DataChunkSplitRDD}
import edu.gmu.stc.climatespark.util.RuntimeMonitor
import edu.gmu.stc.hadoop.index.kdtree.KDTree
import edu.gmu.stc.hadoop.raster._
import edu.gmu.stc.climatespark.functions.ClimateRDDFunctions._
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer
import org.apache.spark.rdd.RDD

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

    println(tree_height)

    val repeatTime = 30

    //println(RuntimeMonitor.show_multi_runtiming(kdTree.range(Array(19860101, 0.0, 0.0, 0.0), Array(19870101, 24.0, 364.0, 576.0)).toArray, repeatTime))

    val start_point = Array(19920101, 0.0, 0.0, 0.0)
    val end_point = Array(19920102, 24.0, 10.0, 43.0)

    val results = kdTree.range(start_point, end_point)

    val dataChunkRDD = qeueryDataChunkRDD(sc, dataChunkMeta, kdTree, start_point, end_point)
    val indexedDataChunkRDD = qeueryIndexedDataChunkRDD(sc, dataChunkMeta, kdTree, start_point, end_point)

    val qqresults = indexedDataChunkRDD.range(start_point, end_point)

    val mapRDD =indexedDataChunkRDD.map(kdTree => kdTree.range(start_point, end_point)).collect()

    println(dataChunkRDD.count())
    println(indexedDataChunkRDD.count())

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

  def qeueryDataChunkRDD(sc: ClimateSparkContext, dataChunkMeta: DataChunkMeta,
                         kdTree: KDTree[DataChunkCoord], start_point: Array[Double], end_point: Array[Double] ): DataChunkRDD = {

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
    dataChunkInputSplitRDD.cache()

    val dataChunkRDD = new DataChunkRDD(dataChunkInputSplitRDD, oneToMorePatitionNumMapping, inputSplitSize, sc.getSparkContext, sc.getHadoopConfig)
    dataChunkRDD
  }

  def qeueryIndexedDataChunkRDD(sc: ClimateSparkContext, dataChunkMeta: DataChunkMeta,
                         kdTree: KDTree[DataChunkCoord], start_point: Array[Double], end_point: Array[Double] ): IndexedDataChunkRDD = {

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
    dataChunkInputSplitRDD.cache()

    val dataChunkRDD = new IndexedDataChunkRDD(dataChunkInputSplitRDD, oneToMorePatitionNumMapping, inputSplitSize, sc.getSparkContext, sc.getHadoopConfig)
    dataChunkRDD
  }
}
