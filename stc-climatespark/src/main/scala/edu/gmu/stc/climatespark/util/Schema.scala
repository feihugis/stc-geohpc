package edu.gmu.stc.climatespark.util

import edu.gmu.stc.hadoop.raster.DataChunk
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import collection.JavaConversions._

/**
  * Created by Fei Hu on 12/27/16.
  */
object Schema {

  def getDataChunkSchema(): StructType = {
    val fields = List(
      //StructField("id", StringType),
      StructField("corner", ArrayType(IntegerType, true)),
      StructField("shape", ArrayType(IntegerType, true)),
      StructField("dimensions", ArrayType(StringType, true)),
      StructField("filePos", LongType),
      StructField("byteSize", LongType),
      StructField("filterMask", IntegerType),
      StructField("hosts", ArrayType(StringType, true)),
      StructField("dataType", StringType),
      StructField("varShortName", StringType),
      StructField("filePath", StringType),
      StructField("time", IntegerType),
      StructField("geometryInfo", StringType)
    )

    StructType(fields)
  }

  def getDataChunkFromRow(row : Row): DataChunk = {

    val corner = row.getAs[List[Int]]("corner").toArray
    val shape = row.getAs[List[Int]]("shape").toArray
    val dims = row.getAs[List[String]]("dimensions").toArray
    val filePos = row.getAs[Long]("filePos")
    val byteSize = row.getAs[Long]("byteSize")
    val filterMask = row.getAs[Int]("filterMask")
    val hosts = row.getAs[List[String]]("hosts").toArray
    scala.util.Sorting.quickSort(hosts)

    val dataType = row.getAs[String]("dataType")
    val varShortName = row.getAs[String]("varShortName")
    val filePath = row.getAs[String]("filePath")
    val time = row.getAs[Int]("time")
    val geometryInfo = row.getAs[String]("geometryInfo")

    new DataChunk(corner, shape, dims, filePos, byteSize, filterMask, hosts,
      dataType, varShortName, filePath, time, geometryInfo)
  }

}
