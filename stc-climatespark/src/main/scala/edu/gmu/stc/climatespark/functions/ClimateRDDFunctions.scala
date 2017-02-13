package edu.gmu.stc.climatespark.functions

import edu.gmu.stc.climatespark.io.datastructure._
import edu.gmu.stc.hadoop.raster.DataChunk
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer
import org.apache.spark.rdd.RDD
import ucar.ma2.{ArrayShort, IndexIterator, MAMath}

import scala.collection.mutable.ArrayBuffer

import scala.math._
/**
  * Created by Fei Hu on 11/16/16.
  */
class ClimateRDDFunctions(self: RDD[(DataChunk, ArraySerializer)]) extends Serializable{

  def queryPointTimeSeries: RDD[Cell] = {
    self.flatMap(tuple => {
      val dataChunk = tuple._1
      val time = dataChunk.getTime
      val shape =  dataChunk.getShape
      val corner = dataChunk.getCorner
      val varName = dataChunk.getVarShortName

      val array = tuple._2.getArray
      var cellList = ArrayBuffer.empty[Cell]

      for (lat:Int <- 0 until shape(0)){
        for (lon:Int <- 0 until shape(1)){
          val index = lat*shape(1)+lon
          val value = array.getShort(index)
          val y = 89.5F - (corner(0)+lat)*1.0F
          val x = -179.5F + (corner(1)+lon)*1.0F
          val cell = Cell(varName, time, 0, y, x, value)
          cellList += cell
        }
      }
      cellList.toList
    })
  }

  def query3DPointTimeSeries: RDD[Cell] = {
    self.filter(tuple => tuple._1 != null)
      .flatMap(tuple => {
      val dataChunk = tuple._1
      val time = dataChunk.getTime
      val shape =  dataChunk.getShape
      val corner = dataChunk.getCorner
      val varName = dataChunk.getVarShortName

      val array = tuple._2.getArray
      var cellList = ArrayBuffer.empty[Cell]

      for (hour:Int <- 0 until shape(0)) {
        for (lat:Int <- 0 until shape(1)){
          for (lon:Int <- 0 until shape(2)){
            val index = hour * shape(1) * shape(2) + lat * shape(2) + lon
            val value = array.getShort(index)
            val y = 89.5F - (corner(1)+lat)*1.0F
            val x = -179.5F + (corner(2)+lon)*1.0F
            val cell = Cell(varName, time, corner(0), y, x, value)
            cellList += cell
          }
        }
      }
      cellList.toList
    })
  }

  def queryWeightPointTimeSeries: RDD[WeightCell] = {
    self.flatMap(tuple => {
      val dataChunk = tuple._1
      val time = dataChunk.getTime.toString
      val shape =  dataChunk.getShape
      val corner = dataChunk.getCorner
      val varName = dataChunk.getVarShortName

      val array = tuple._2.getArray
      var cellList = ArrayBuffer.empty[WeightCell]

      for (lat:Int <- 0 until shape(0)){
        for (lon:Int <- 0 until shape(1)){
          val index = lat*shape(1)+lon
          val value = array.getShort(index)
          val y = 89.5F - (corner(0)+lat)*1.0F
          val x = -179.5F + (corner(1)+lon)*1.0F
          val weight = (2*Pi*6371*6371*abs(sin((y - 0.5)/180*Pi) - sin((y + 0.5)/180*Pi)) * 1).toFloat
          val cell = WeightCell(varName, time, y, x, value, weight)
          cellList += cell
        }
      }
      cellList.toList
    })
  }

  def avgDaily: RDD[String] = {
    self.map(tuple => {
      val key = tuple._1.getVarShortName + "_" + tuple._1.getTime
      val value = MAMath.sumDouble(tuple._2.getArray)
      key + " : " + value
    })
  }

  def weightedAreaAvgDaily: RDD[String] = {
    self.map(tuple => {
      val key = tuple._1.getVarShortName + "_" + tuple._1.getTime
      val corner = tuple._1.getCorner
      val shape = tuple._1.getShape
      val array = tuple._2.getArray
      var numerator = 0D
      var denominator = 0D

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
        }}
      key + " : " + numerator/denominator
    })
  }

  def timeAvg(varNum: Int): RDD[(String, ArraySerializer)] = {
    val num = (self.count() / varNum).toInt
    self.map(tuple => {
      val key = tuple._1.getVarShortName
      (key, tuple._2)
    }).reduceByKey((array1, array2) => {
      val a = array1.getArray
      val b = array2.getArray
      val result = ucar.ma2.Array.factory(a.getElementType, a.getShape);

      val iterR = result.getIndexIterator
      val iterA = a.getIndexIterator
      val iterB = b.getIndexIterator

      while (iterA.hasNext()) {
        iterR.setIntNext(iterA.getIntNext + iterB.getIntNext)
      }

      ArraySerializer.factory(result)
    }).mapValues(array => {
      val a = array.getArray
      val result = ucar.ma2.Array.factory(a.getElementType(), a.getShape());
      val iterA = a.getIndexIterator
      val iterR = result.getIndexIterator

      while (iterA.hasNext) {
        iterR.setIntNext(iterA.getIntNext / num)
      }

      ArraySerializer.factory(result)
    })
  }

  def joinVars:RDD[MultiCell] = {
    self.map(cRDD => (cRDD._1.getTime, cRDD)).groupByKey().flatMap(u => {
      val varsList = u._2.toList.sortBy(_._1.getVarShortName)
      val var_0 = varsList(0);
      val corner = var_0._1.getCorner
      val shape = var_0._1.getShape
      val valueType = var_0._2.getArray.getElementType
      val time = var_0._1.getTime
      val multiCellList = ArrayBuffer.empty[MultiCell]

      for (y: Int <- 0 until shape(0)) {
        for (x: Int <- 0 until shape(1)) {
          val values = ArrayBuffer.empty[Short]
          val lat = 89.5F - (corner(0) + y)*1.0F
          val lon = -179.5F + (corner(1) + x)*0.5F
          val index = y * shape(1) + x
          varsList.foreach(cRDD => values += cRDD._2.getArray.getShort(index))
          multiCellList += MultiCell.factory(lat, lon, time, values.toList)
        }
      }
      multiCellList.toList
    })
  }
}

object ClimateRDDFunctions {

  implicit def fromRDD(rdd: RDD[(DataChunk,ArraySerializer)]): ClimateRDDFunctions = new ClimateRDDFunctions(rdd)
}
