package edu.gmu.stc.climatespark.functions


import java.io.{InputStreamReader, BufferedReader}
import java.util

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import edu.gmu.stc.hadoop.index.kdtree.KDTree
import edu.gmu.stc.hadoop.raster.{DataChunkCorner, DataChunkByteLocation, DataChunkCoord, DataChunk}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path, FileSystem}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}
import edu.gmu.stc.climatespark.util.{StringUtils, Schema, RuntimeMonitor}
import scala.collection.mutable.ArrayBuffer
import scala.reflect._

import scala.reflect.ClassTag

/**
  * Created by Fei Hu on 12/28/16.
  */
object DataFormatFunctions {
  def csvToParquet(inputFile : String, outputFile:String, partitionNum: Int, sc : SparkContext) = {
    val sqlContext = new SQLContext(sc)
    val rdd = sc.textFile(inputFile).map(r => new DataChunk(r)).map(atts =>
      (atts.getId, Row(atts.getCorner, atts.getShape, atts.getDimensions, atts.getFilePos,
        atts.getByteSize, atts.getFilterMask, atts.getHosts, atts.getDataType,
        atts.getVarShortName, atts.getFilePath, atts.getTime, atts.getGeometryInfo)))
        .repartition(partitionNum).sortByKey().map(d => d._2)

    val schema = Schema.getDataChunkSchema()
    val rowDF = sqlContext.createDataFrame(rdd, schema)
    rowDF.saveAsParquetFile(outputFile)
  }

  def parquetToDataChunkRDD(sc : SparkContext, inputFile : String) : RDD[DataChunk] = {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.parquet(inputFile)
    df.map(r => Schema.getDataChunkFromRow(r))
  }

  def csvToDataChunkRDD(inputFile : String, sc : SparkContext): RDD[DataChunk] = {
    sc.textFile(inputFile).map(r => new DataChunk(r))
  }

  def parquetToDataChunkCoordRDD(sc : SparkContext, inputFile : String) : RDD[DataChunkCoord] = {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.parquet(inputFile)
    df.map(r => Schema.getDataChunkFromRow(r)).map(dataChunk => new DataChunkCoord(dataChunk)).filter(d => {
      val coord = d.getCorner
      if (coord(0) > 19911231) {
        true
      } else {
        false
      }
    })
  }

  def csvToDataChunkCoordRDD(sc : SparkContext, inputFile : String): RDD[DataChunkCoord] = {
    this.csvToDataChunkRDD(inputFile, sc).map(dataChunk => new DataChunkCoord(dataChunk)).filter(d => {
      val coord = d.getCorner
      if (coord(0) > 19911231) {
        true
      } else {
        false
      }
    })
  }

  def parquetToKDTreeFile(sc : SparkContext, config: Configuration, inputFile : String, outputFile: String) = {
    val dataChunkCoordRDD = DataFormatFunctions.parquetToDataChunkCoordRDD(sc, inputFile)
    val coordArray = dataChunkCoordRDD.collect()
    val kdTree = new KDTree[DataChunkCoord](coordArray(0).getCorner.length)

    coordArray.foreach(dataChunkCoor => {
      kdTree.insert(dataChunkCoor.getCornerAsDouble, dataChunkCoor)
    })

    val gson = new Gson();
    val kdTreeString = gson.toJson(kdTree, new TypeToken[KDTree[DataChunkCoord]](){}.getType)
    val fs = FileSystem.get(config)
    val fin = fs.create(new Path(outputFile))
    fin.writeBytes(kdTreeString)
    fin.close()
  }

  def csvToKDTreeFile(sc : SparkContext, config: Configuration, inputFile : String, outputFile: String) = {
    val dataChunkCoordRDD = DataFormatFunctions.csvToDataChunkCoordRDD(sc, inputFile)
    val coordArray = dataChunkCoordRDD.collect()
    val kdTree = new KDTree[DataChunkCoord](coordArray(0).getCorner.length)

    coordArray.foreach(dataChunkCoor => {
      kdTree.insert(dataChunkCoor.getCornerAsDouble, dataChunkCoor)
    })

    val gson = new Gson();
    val kdTreeString = gson.toJson(kdTree, new TypeToken[KDTree[DataChunkCoord]](){}.getType)
    val fs = FileSystem.get(config)
    val fin = fs.create(new Path(outputFile))
    fin.writeBytes(kdTreeString)
    fin.close()
  }

  def kdTreeFileToKDTree(sc : SparkContext, config: Configuration, inputFile : String): KDTree[DataChunkCoord] = {
    val fs = FileSystem.get(config)
    val inputStream = fs.open(new Path(inputFile))
    val br = new BufferedReader(new InputStreamReader(inputStream))
    val gson = new Gson()
    gson.fromJson(br, new TypeToken[KDTree[DataChunkCoord]](){}.getType).asInstanceOf[KDTree[DataChunkCoord]]
  }

  def csvToDataNodeIndexFile(inputFile : String, outputFilePath: String, varName: String, partitionNum: Int, sc: SparkContext) = {
    val rdd = sc.textFile(inputFile).map(r => new DataChunk(r))

    val grouppedDataChunksByHost = rdd.flatMap(dataChunk => {
      val hosts = dataChunk.getHosts
      val key = new DataChunkCorner(dataChunk).getID
      val value = new DataChunkByteLocation(dataChunk)
      val results = new Array[(Int, Array[(Double, DataChunkByteLocation)])](hosts.size)
      for ( i <- 0 until hosts.size) {
        val h = hosts(i)
        val hostID = StringUtils.getHostID(h)
        results(i) = (hostID, Array((key, value)))
      }
      results
    }).repartition(partitionNum)
      .reduceByKey((v1, v2) => {
      v1 ++ v2
    }).mapValues(dataChunks => {
       val dataChunkHashMap = new util.HashMap[java.lang.Double, DataChunkByteLocation]()
       dataChunks.foreach(datachunk => {
         val key = datachunk._1
         val value = datachunk._2
         dataChunkHashMap.put(key, value)
       })
      dataChunkHashMap
    }).foreach(p => {
      val gson = new Gson();
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      val indexFile = new Path(outputFilePath + "/" + varName + "_" + p._1 + ".txt")
      if (!fs.exists(indexFile)) {
        val fin = fs.create(indexFile)
        fin.writeBytes(gson.toJson(p._2, new TypeToken[util.HashMap[java.lang.Double, DataChunkByteLocation]](){}.getType))
        fin.close()
      }
    })
  }
}
