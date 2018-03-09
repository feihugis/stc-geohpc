package edu.gmu.stc.climatespark.application

import edu.gmu.stc.climatespark.core.ClimateSparkContext
import edu.gmu.stc.climatespark.io.datastructure.{Cell4D, Cell5D}
import edu.gmu.stc.climatespark.io.util.NetCDFUtils._
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Fei Hu on 3/29/17.
  */
object GenericFileCluster {
  val sc = new SparkContext()
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val configFile =  "/var/lib/hadoop-hdfs/1006/mod08-climatespark-config.xml"
  val timePattern = """\d\d\d\d\d\d\d"""
  val varFullName = "mod08/Data_Fields/Cirrus_Reflectance_Histogram_Counts"
  val varShortName = "Cirrus_Reflectance_Histogram_Counts"

  val climateSparkSC = new ClimateSparkContext(sc, configFile)
  val climateRDD = climateSparkSC.getClimateRDD
  val cellResult1 = climateRDD.flatMap{case (dataChunk, arraySerializer) => {
    val cells = ArrayBuffer.empty[Cell5D]
    val array = arraySerializer.getArray
    val index = array.getIndex
    val time = dataChunk.getTime
    val shape = array.getShape
    val corner: Array[Int] = Array(0, 0, 0, 0)
    for (d1 <- corner(0) until corner(0) + shape(0)) {
      for (d2 <- corner(1) until corner(1) + shape(1)) {
        for (d3 <- corner(2) until corner(2) + shape(2)) {
          for (d4 <- corner(3) until corner(3) + shape(3)) {
            val value = array.getShort(index.set(d1, d2, d3, d4))
            val cell = Cell5D(time, d1, d2, d3, d4, value)
            cells += cell
          }
        }
      }
    }

    cells.toList
  }}

  val cellResult: RDD[Cell4D] = climateRDD.flatMap{case (dataChunk, arraySerializer) => {
    val cells = ArrayBuffer.empty[Cell4D]
    val array = arraySerializer.getArray
    val index = array.getIndex
    val time = dataChunk.getTime
    val shape = array.getShape
    val corner: Array[Int] = Array(0, 0, 0)
    for (d1 <- corner(0) until corner(0) + shape(0)) {
      for (d2 <- corner(1) until corner(1) + shape(1)) {
        for (d3 <- corner(2) until corner(2) + shape(2)) {
          val value = array.getShort(index.set(d1, d2, d3))
          val cell = Cell4D(time, d1, d2, d3, value)
          cells += cell
        }
      }
    }
    cells.toList
  }}

  cellResult.toDF().saveAsParquetFile("/" + varShortName + "2000061_2016061" +".parquet")

  val genericFileRDD: RDD[(Text, Text)] = climateSparkSC.getGenericFileRDD

  val arrayRDD: RDD[(Int, ArraySerializer)] = genericFileRDD.getArray(varFullName, timePattern)

  val cellRDD = arrayRDD.flatMap {case (time, arraySerializer) => {
    val cells = ArrayBuffer.empty[Cell5D]
    val array = arraySerializer.getArray
    val index = array.getIndex
    val shape = array.getShape
    val corner: Array[Int] = Array(0, 0, 0, 0)
    for (d1 <- corner(0) until corner(0) + shape(0)) {
      for (d2 <- corner(1) until corner(1) + shape(1)) {
        for (d3 <- corner(2) until corner(2) + shape(2)) {
          for (d4 <- corner(3) until corner(3) + shape(3)) {
            val value = array.getShort(index.set(d1, d2, d3, d4))
            val cell = Cell5D(time, d1, d2, d3, d4, value)
            cells += cell
          }
        }
      }
    }
    cells.toList
  }}







}
