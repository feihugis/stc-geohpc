package edu.gmu.stc.climatespark.application

import edu.gmu.stc.climatespark.core.ClimateSparkContext
import edu.gmu.stc.climatespark.io.ClimateSparkKryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}
import edu.gmu.stc.climatespark.io.util.NetCDFUtils._
import edu.gmu.stc.database.DBConnector
import edu.gmu.stc.hadoop.commons.{ClimateDataMetaParameter, ClimateHadoopConfigParameter}
import edu.gmu.stc.hadoop.raster.index.DataChunkIndexBuilderImp

/**
  * Created by Fei Hu on 3/29/17.
  */
object GenericFileTest {
  def main(args: Array[String]): Unit = {
    val configFile =  "/var/lib/hadoop-hdfs/1006/mod08-climatespark-config.xml"  //args(0) //"/Users/feihu/Documents/GitHub/stc-geohpc/stc-spark/src/main/resources/merra2-climatespark-config.xml"
    //val climateSparkSC = /*new ClimateSparkContext(sc, configFile)*/ new ClimateSparkContext(configFile, "local[6]", "test")
    val climateSparkSC = new ClimateSparkContext(sc, configFile)
    val genericFileRDD = climateSparkSC.getGenericFileRDD
    genericFileRDD.foreach(tuple => print(tuple._1.toString + ":" + tuple._2.toString))

    val files = genericFileRDD.collect()

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

    val test = genericFileRDD.getArray("PRECCU")
    //test.foreach(array => println(array.getArray.getSize))


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

    val result = genericFileRDD.buildIndex("Cirrus_Reflectance_Histogram_Counts",
      filePath_prefix, filePath_suffix,
      dbHost, dbPort, dbName, dbUser, dbPWD)
    println(result.reduce(_+_))
  }
}
