package edu.gmu.stc.climatespark.functions

import edu.gmu.stc.climatespark.io.datastructure.Cell
import edu.gmu.stc.hadoop.raster.DataChunk
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer
import org.apache.spark.rdd.RDD
import ucar.ma2.{IndexIterator, MAMath}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Fei Hu on 11/16/16.
  */
class ClimateRDDFunctions(self: RDD[(DataChunk, ArraySerializer)]) extends Serializable{

  def queryPointTimeSeries: RDD[Cell] = {
    self.flatMap(tuple => {
      val dataChunk = tuple._1
      val time = dataChunk.getTime.toString
      val shape =  dataChunk.getShape
      val corner = dataChunk.getCorner
      val varName = dataChunk.getVarShortName

      val array = tuple._2.getArray
      var cellList = ArrayBuffer.empty[Cell]

      for (lat:Int <- 0 until shape(0)){
        for (lon:Int <- 0 until shape(1)){
          val index = lat*shape(1)+lon
          val value = array.getShort(index)
          val y = -90 + (corner(0)+lat)*0.5F
          val x = -180 + (corner(1)+lon)*0.5F
          val cell = Cell(varName, time, y, x, value)
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

      while(iterA.hasNext()) {
        iterR.setIntNext(iterA.getIntNext + iterB.getIntNext)
      }

      ArraySerializer.factory(result)
    }).mapValues(array => {
      val a = array.getArray
      val result = ucar.ma2.Array.factory(a.getElementType(), a.getShape());
      val iterA = a.getIndexIterator
      val iterR = result.getIndexIterator

      while (iterA.hasNext) {
        iterR.setIntNext(iterA.getIntNext/num)
      }

      ArraySerializer.factory(result)
    })
  }


}

object ClimateRDDFunctions {

  implicit def fromRDD(rdd: RDD[(DataChunk,ArraySerializer)]): ClimateRDDFunctions = new ClimateRDDFunctions(rdd)
}
