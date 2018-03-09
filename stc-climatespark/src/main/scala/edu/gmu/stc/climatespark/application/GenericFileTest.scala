package edu.gmu.stc.climatespark.application

import edu.gmu.stc.climatespark.core.ClimateSparkContext
import edu.gmu.stc.climatespark.io.ClimateSparkKryoRegistrator
import edu.gmu.stc.climatespark.io.datastructure.{Cell4D, Cell5D}
import org.apache.spark.{SparkConf, SparkContext}
import edu.gmu.stc.climatespark.io.util.NetCDFUtils._
import edu.gmu.stc.database.DBConnector
import edu.gmu.stc.hadoop.commons.{ClimateDataMetaParameter, ClimateHadoopConfigParameter}
import edu.gmu.stc.hadoop.raster.index.DataChunkIndexBuilderImp
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Fei Hu on 3/29/17.
  */
object GenericFileTest {

  def buildIndex(genericFileRDD: RDD[(Text, Text)], varName: String, climateSparkSC: ClimateSparkContext) = {
    val configuration = climateSparkSC.getHadoopConfig
    val inputDir = configuration.get(ClimateHadoopConfigParameter.MR_FILEINPUTFORMAT_INPUT_DIR)

    val productName = configuration.get(ClimateDataMetaParameter.CLIMATE_DATA_PRODUCT)
    val filePath_prefix = configuration.get(ClimateHadoopConfigParameter.INDEX_FILEPATH_PREFIX)
    val filePath_suffix = configuration.get(ClimateHadoopConfigParameter.INDEX_FILEPATH_SUFFIX)
    val inputFile_Filter = configuration.get(ClimateHadoopConfigParameter.INDEX_INPUTFILE_FILTER)
    val index_variables = configuration.get(ClimateHadoopConfigParameter.INDEX_VARIABLES)
    val fileFormat = configuration.get(ClimateDataMetaParameter.CLIMATE_DATA_FORMAT)
    val groupName = configuration.get(ClimateDataMetaParameter.CLIMATE_DATA_PRODUCT_GROUP)
    val dbHost = configuration.get(ClimateHadoopConfigParameter.DB_HOST)
    val dbPort = configuration.get(ClimateHadoopConfigParameter.DB_Port)
    val dbName = configuration.get(ClimateHadoopConfigParameter.DB_DATABASE_NAME)
    val dbUser = configuration.get(ClimateHadoopConfigParameter.DB_USERNAME)
    val dbPWD = configuration.get(ClimateHadoopConfigParameter.DB_PWD)
    val spark_Master = configuration.get(ClimateHadoopConfigParameter.SPARK_MASTER)

    val dataChunkIndexBuilderImp: DataChunkIndexBuilderImp = new DataChunkIndexBuilderImp(filePath_prefix, filePath_suffix)
    var statement = new DBConnector(dbHost, dbPort, dbUser, dbPWD).GetConnStatement
    dataChunkIndexBuilderImp.createDatabase(dbName, dbUser, statement)
    statement.getConnection.close()
    statement = new DBConnector(dbHost, dbPort, dbName, dbUser, dbPWD).GetConnStatement

    val varNames = index_variables.split(",")

    for (varName <- varNames) {
      dataChunkIndexBuilderImp.createTable(varName, statement)
    }
    statement.getConnection.close()

    val result = genericFileRDD.buildIndex(varNames,
      filePath_prefix, filePath_suffix,
      dbHost, dbPort, dbName, dbUser, dbPWD)
    println(result.reduce(_+_))

  }



  def main(args: Array[String]): Unit = {
    val configFile =  "/Users/feihu/Documents/GitHub/stc-geohpc/stc-spark/src/main/resources/merra2-climatespark-config.xml"
    val climateSparkSC = new ClimateSparkContext(configFile, "local[6]", "test")
    val sqlContext = new SQLContext(climateSparkSC.getSparkContext)
    import sqlContext.implicits._

    val genericFileRDD = climateSparkSC.getGenericFileRDD
    val varShortName = "T2M"
    val timePattern = """\d\d\d\d\d\d\d"""
    val fullVarName = "T2M"

    buildIndex(genericFileRDD, varShortName, climateSparkSC)

    /*
    val arrayRDD: RDD[(Int, ArraySerializer)] = genericFileRDD.getArray(fullVarName, timePattern)
    arrayRDD.foreach{ case (time, array) => println(time)}

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

    val df = cellRDD.toDF()
    df.write.format("com.databricks.spark.csv").save("test.csv")*/

  }
}
