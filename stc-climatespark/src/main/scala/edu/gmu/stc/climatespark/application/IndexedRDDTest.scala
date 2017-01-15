package edu.gmu.stc.climatespark.application

import edu.gmu.stc.climatespark.core.ClimateSparkContext
import edu.gmu.stc.climatespark.functions.DataFormatFunctions
import edu.gmu.stc.climatespark.rdd.{DataChunkRDD, DataChunkSplitRDD}
import edu.gmu.stc.hadoop.raster._



/**
  * Created by Fei Hu on 12/26/16.
  */
object IndexedRDDTest {

  def main(args: Array[String]) {
    val configFile = "/Users/feihu/Documents/GitHub/stc-geohpc/stc-spark/src/main/resources/merra2-climatespark-config.xml"
    val sc = new ClimateSparkContext(configFile, "local[6]", "test")
    //val sparkConf = new SparkConf().setMaster("local[6]").setAppName("test").set("spark.driver.allowMultipleContexts", "true")

    val dataChunkMeta = new DataChunkMeta(Array(1, 1, 91, 144), Array("date", "hour", "lat", "lon"), 0, "float", "PRECCU", "/Users/feihu/Documents/Data/Merra2/MERRA2_200.tavg1_2d_int_Nx.*.nc4")

    val inputFile = "/Users/feihu/Documents/Data/Merra2/index/preccu"
    val outputFile = "/Users/feihu/Documents/Data/Merra2/index/preccu-index-test.parquet"
    val kdTreeFile = "/Users/feihu/Documents/Data/Merra2/index/kdindexGson.txt"
    //DataFormatFunctions.csvToParquet(inputFile, outputFile, 100, sc.getSparkContext)
    //val dataChunkRDD = DataFormatFunctions.parquetToDataChunkRDD(sc.getSparkContext, outputFile)
    //val dataChunkCoordRDD = DataFormatFunctions.parquetToDataChunkCoordRDD(sc.getSparkContext, outputFile)
    //DataFormatFunctions.csvToDataNodeIndexFile(inputFile, "/Users/feihu/Documents/Data/Merra2/index/nodeindex/", "preccu", 23, sc.getSparkContext)


    val kdTree = DataFormatFunctions.kdTreeFileToKDTree(sc.getSparkContext, sc.getHadoopConfig, kdTreeFile)

    val dataChunkCoordResults = kdTree.range(Array(19920101, 0.0, 0.0, 0.0), Array(19920102, 24.0, 364.0, 576.0)).toArray

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
    }).toArray

    val dataChunkInputSplitRDD = new DataChunkSplitRDD(scheduledDataChunks, dataChunkMeta, sc.getSparkContext, sc.getHadoopConfig)
    val dataChunkRDD = new DataChunkRDD(dataChunkInputSplitRDD, sc.getSparkContext, sc.getHadoopConfig)
    val paritions = dataChunkRDD.partitions
    val size = dataChunkRDD.count()
    val dependency = dataChunkRDD.dependencies
    println("--------" + dataChunkRDD.dependencies)
  }
}
