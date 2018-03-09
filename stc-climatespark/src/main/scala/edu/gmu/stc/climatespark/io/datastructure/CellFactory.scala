package edu.gmu.stc.climatespark.io.datastructure

import edu.gmu.stc.hadoop.raster.DataChunk
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.math.{abs, sin, Pi}

/**
  * Created by Fei Hu on 4/14/17.
  */
class CellFactory (self: (DataChunk, ArraySerializer)) extends Serializable{

  def getCells:List[Cell] = {
    val dataChunk = self._1
    val time = dataChunk.getTime
    val shape =  dataChunk.getShape
    val corner = dataChunk.getCorner
    val varName = dataChunk.getVarShortName

    val array = self._2.getArray
    val cellList = ArrayBuffer.empty[Cell]

    shape.length match {
      case 2 =>
        for (lat:Int <- 0 until shape(0)){
          for (lon:Int <- 0 until shape(1)){
            val index = lat*shape(1)+lon
            val value = array.getShort(index)
            val cell = Cell3D(time, corner(0) + lat, corner(1) + lon, value)
            cellList += cell
          }
        }

      case 3 =>
        for (d1: Int <- 0 until shape(0)) {
          for (lat: Int <- 0 until shape(1)) {
            for (lon: Int <- 0 until shape(2)) {
              val index = d1*shape(1)*shape(2) + lat * shape(2) + lon
              val value = array.getShort(index)
              val cell = Cell4D(time, corner(0) + d1, corner(1) + lat, corner(2) +lon, value)
              cellList += cell
            }
          }
        }

      case 4 =>
        for (d1: Int <- 0 until shape(0)) {
          for (d2: Int <- 0 until shape(1)) {
            for (lat: Int <- 0 until shape(2)) {
              for (lon: Int <- 0 until shape(3)) {
                val index = d1 * shape(1) * shape(2) * shape(3) + d2 * shape(2) * shape(3) + lat * shape(3) + lon
                val value = array.getShort(index)
                val cell = Cell5D(time, corner(0) + d1, corner(1) + d2, corner(2) + lat, corner(2) + lon, value)
                cellList += cell
              }
            }
          }
        }
    }
    cellList.toList
  }

  def calWeightedAreaAvgDaily: List[String] = {
    val key = self._1.getVarShortName + "_" + self._1.getTime
    val corner = self._1.getCorner
    val shape = self._1.getShape
    val array = self._2.getArray
    var numerator = 0D
    var denominator = 0D

    val avgList = ArrayBuffer.empty[String]

    shape.length match {
      case 2 =>
        for (lat:Int <- 0 until shape(0)){
          for (lon:Int <- 0 until shape(1)){
            val index = lat*shape(1)+lon
            val value = array.getShort(index)
            if ( value != -9999) {
              val y = 89.5F - (corner(0)+lat)*1.0F
              val x = -179.5F + (corner(1)+lon)*1.0F
              val area = 2*Pi*6371*6371*abs(sin((y - 0.5)/180*Pi) - sin((y + 0.5)/180*Pi)) * 1
              numerator = numerator + area * value
              denominator = denominator + area
            }
          }
        }
        avgList += (key + " : " + numerator/denominator)

      case 3 =>
        for (d1:Int <- 0 until shape(0)) {
          for (lat: Int <- 0 until shape(1)) {
            for (lon: Int <- 0 until shape(2)) {
              val index = d1 * shape(1) * shape(2) + lat * shape(2) + lon
              val value = array.getShort(index)
              if (value != -9999) {
                val y = 89.5F - (corner(1) + lat) * 1.0F
                val x = -179.5F + (corner(2) + lon) * 1.0F
                val area = 2 * Pi * 6371 * 6371 * abs(sin((y - 0.5) / 180 * Pi) - sin((y + 0.5) / 180 * Pi)) * 1
                numerator = numerator + area * value
                denominator = denominator + area
              }
            }
          }
          avgList += (key + " : " + numerator / denominator)
        }

      case 4 =>
        for (d1:Int <- 0 until shape(0)) {
          for (d2:Int <- 0 until shape(1)) {
            for (lat: Int <- 0 until shape(2)) {
              for (lon: Int <- 0 until shape(3)) {
                val index = d1 * shape(1) * shape(2) * shape(3) + d2 * shape(2) * shape(3) + lat * shape(3) + lon
                val value = array.getShort(index)
                if (value != -9999) {
                  val y = 89.5F - (corner(2) + lat) * 1.0F
                  val x = -179.5F + (corner(3) + lon) * 1.0F
                  val area = 2 * Pi * 6371 * 6371 * abs(sin((y - 0.5) / 180 * Pi) - sin((y + 0.5) / 180 * Pi)) * 1
                  numerator = numerator + area * value
                  denominator = denominator + area
                }
              }
            }
            avgList += (key + " : " + numerator / denominator)
          }
        }
    }
    avgList.toList
    }
}

object CellFactory {
  implicit def fromDataChunk(dataChunk: (DataChunk, ArraySerializer)):CellFactory = new CellFactory(dataChunk)
}
