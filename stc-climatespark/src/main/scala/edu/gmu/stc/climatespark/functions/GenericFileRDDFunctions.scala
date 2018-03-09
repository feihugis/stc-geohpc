package edu.gmu.stc.climatespark.functions

import edu.gmu.stc.climatespark.core.ClimateSparkContext
import edu.gmu.stc.database.DBConnector
import edu.gmu.stc.hadoop.commons.{ClimateDataMetaParameter, ClimateHadoopConfigParameter}
import edu.gmu.stc.hadoop.raster.DataChunk
import edu.gmu.stc.hadoop.raster.index.DataChunkIndexBuilderImp
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import edu.gmu.stc.climatespark.io.util.NetCDFUtils._
import org.apache.hadoop.conf.Configuration

/**
  * Created by Fei Hu on 4/3/17.
  */
class GenericFileRDDFunctions (self: RDD[(Text, Text)]) extends Serializable{

  def buildIndex(configuration: Configuration): String = {
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

      dataChunkIndexBuilderImp.createTable(varName.trim, statement)
    }
    statement.getConnection.close()

    val result = self.buildIndex(varNames, filePath_prefix, filePath_suffix,
                                  dbHost, dbPort, dbName, dbUser, dbPWD)
    "Build the index for %d files".format(result.reduce(_ + _))
  }

  //inputDatePattern = """\d\d\d\d\d\d\d"""
  def filterFileByTime(inputDatePattern: String, startTime: Int, endTime: Int): RDD[(Text, Text)] = {
    self.filter{ case (filePath, hosts) => {
      val datePattern = inputDatePattern.r
      val curDate = datePattern.findFirstIn(filePath.toString).getOrElse("99999999").toInt

      if (curDate >= startTime && curDate <= endTime) {
        true
      } else {
        false
      }
    }

    }
  }

}

object GenericFileRDDFunctions {
  implicit def fromRDD(rdd: RDD[(Text, Text)]): GenericFileRDDFunctions = new GenericFileRDDFunctions(rdd)
}
