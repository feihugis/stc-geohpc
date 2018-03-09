package edu.gmu.stc.climatespark.application

import com.google.gson.{Gson, GsonBuilder}
import edu.gmu.stc.climatespark.core.ClimateSparkContext
import org.apache.spark.sql.SQLContext
import edu.gmu.stc.hadoop.vector.geojson.{ZikaGeoJSON, ZikaPolygonJSON}
import java.io.{BufferedReader, FileReader}

import edu.gmu.stc.climatespark.io.datastructure.{ArrayBbox2D, ArrayBbox3D}
import edu.gmu.stc.hadoop.raster.ChunkUtils
import ucar.ma2.MAMath

import collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType


/**
  * Created by Fei Hu on 4/2/17.
  */
object GeoRasterExtract {

  def main(args: Array[String]): Unit = {
    val configFile =  "/Users/feihu/Documents/GitHub/stc-geohpc/stc-spark/src/main/resources/merra2-climatespark-config.xml"
    val geojsonFile = "/Users/feihu/Google Drive/Projects/ClimateHealth-ML-DL/data/zika_sites.geojson"
    val climateSparkSC = new ClimateSparkContext(configFile, "local[6]", "test")
    val sqlContext = new SQLContext(climateSparkSC.getSparkContext)

    import sqlContext.implicits._

    val corner = Array(0, -90.0, -180.0)
    val resolution = Array(1.0, 0.5, 0.625)

    val br = new BufferedReader(new FileReader(geojsonFile))
    val gson = new GsonBuilder().create
    val polygonFeatures = gson.fromJson(br, classOf[ZikaGeoJSON]).getFeatures.asScala

    val stateBboxes = polygonFeatures.map(feature => {
      val bbox = feature.getBbox
      val corner0_P1 = ((bbox(1) - corner(1))/resolution(1)).toInt
      val corner1_P1 = ((bbox(0) - corner(2))/resolution(2)).toInt
      val corner0_P2 = ((bbox(3) - corner(1))/resolution(1)).toInt
      val corner1_P2 = ((bbox(2) - corner(2))/resolution(2)).toInt
      val corner0 = math.min(corner0_P1, corner0_P2)
      val corner1 = math.min(corner1_P1, corner1_P2)
      val shape0 = math.abs(corner0_P1 - corner0_P2) + 1
      val shape1 = math.abs(corner1_P1 - corner1_P2) + 1
      ArrayBbox3D(feature.getProperties.getNAME, 0, corner0, corner1, 24, shape0, shape1)
    })

    println(stateBboxes.length)

    val arrayBbox3D = climateSparkSC.getSparkContext.broadcast(stateBboxes)

    val chunkRDD = climateSparkSC.getClimateRDD

    val sumRDD = chunkRDD.flatMap{case (dataChunk, arraySerializable) => {
      val result = ArrayBuffer.empty[(String, String, Int, Double, Double)]
      for (inputBbox <- arrayBbox3D.value) {
        val queryCorner = Array(inputBbox.corner0, inputBbox.corner1, inputBbox.corner2)
        val queryShape = Array(inputBbox.shape0, inputBbox.shape1, inputBbox.shape2)
        val targetCorner = new Array[Int](queryCorner.length)
        val targetShape = new Array[Int](queryCorner.length)
        if (ChunkUtils.getIntersection(dataChunk.getCorner, dataChunk.getShape, queryCorner, queryShape, targetCorner, targetShape)) {
          val relativeCorner = new Array[Int](targetCorner.length)
          for (i <- relativeCorner.indices) {
            relativeCorner(i) = targetCorner(i) - dataChunk.getCorner.toList(i)
          }
          val subArray = arraySerializable.getArray.section(relativeCorner, targetShape).copy()
          val iterA = subArray.getIndexIterator
          var sum = 0.0
          var count = 0.0
          while (iterA.hasNext) {
            val value = iterA.getDoubleNext
            if (value < 10000) {
              sum = sum + value
              count = count + 1.0
            }
          }
          result += Tuple5(inputBbox.state, dataChunk.getVarShortName,
            (dataChunk.getTime + "%02d").format(dataChunk.getCorner.toList.head).toInt, sum, count)
        }
      }

      val length = result.length
      result.toList
    }}.filter(tuple => tuple != null)

    val avgRDD = sumRDD.groupBy{case (state, varName, time, sum, count) => (state, varName, time)}
                       .map{case (key, iterator) =>
                         var sum = 0.0
                         var count = 0.0
                         for (tuple <- iterator) {
                           sum = sum + tuple._4
                           count = count + tuple._5
                         }
                         (key._1, key._2, key._3, sum/count)
                       }


    val customSchema = new StructType(Array[StructField](
      StructField("state", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("var", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("time", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("value", DataTypes.DoubleType, nullable = true, Metadata.empty)))

    //val df = avgRDD.toDF("state", "var", "time", "value")
    /*val df = avgRDD.toDF()

    df.write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("test.csv")

    println(avgRDD.count())*/
  }

}
