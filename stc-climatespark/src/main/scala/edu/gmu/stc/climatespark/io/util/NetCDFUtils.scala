package edu.gmu.stc.climatespark.io.util

import java.sql.Statement
import java.util

import edu.gmu.stc.database.DBConnector
import edu.gmu.stc.hadoop.index.io.merra.NcHdfsRaf
import edu.gmu.stc.hadoop.raster.{ChunkUtils, DataChunk}
import edu.gmu.stc.hadoop.raster.index.DataChunkIndexBuilderImp
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.{FileSystem, Path}
import ucar.nc2.NetcdfFile
import scala.collection.JavaConversions._


/**
  * Created by Fei Hu on 3/29/17.
  */
class NetCDFUtils (self: RDD[(Text, Text)]) extends Serializable{


  def getArray(varName: String): RDD[ArraySerializer] = {
    self.map(tuple => {
      val path = tuple._1
      val conf = new Configuration
      val fs = FileSystem.get(conf)
      val raf = new NcHdfsRaf(fs.getFileStatus(new Path(path.toString)), fs.getConf)
      val netcdfFile = NetcdfFile.open(raf, path.toString)
      val variable = netcdfFile.findVariable(varName)
      val array = variable.read()
      ArraySerializer.factory(array)
    })
  }

  def buildIndex(varName: String,
                 filePath_prefix: String, filePath_suffix: String,
                 dbHost: String, dbPort: String, dbName: String,
                 dbUser: String, dbPWD: String) = self.map(tuple => {
    val filePath = tuple._1.toString
    val dataChunkIndexBuilderImp: DataChunkIndexBuilderImp = new DataChunkIndexBuilderImp(filePath_prefix, filePath_suffix)
    val statement: Statement = new DBConnector(dbHost, dbPort, dbName, dbUser, dbPWD).GetConnStatement
    val chunkList = new util.ArrayList[DataChunk]

    chunkList.addAll(ChunkUtils.geneDataChunksByVar(filePath, varName))
    dataChunkIndexBuilderImp.insertDataChunks(chunkList, varName, statement)
    statement.getConnection.close()
    1
  })
}

object NetCDFUtils {
  implicit def fromRDD(rdd: RDD[(Text,Text)]): NetCDFUtils = new NetCDFUtils(rdd)

  def main(args: Array[String]): Unit = {
    val path = "/Users/feihu/Documents/Data/modis_hdf/MOD08_D3/MOD08_D3.A2016001.006.2016008061022.hdf"
    val conf = new Configuration
    val fs = FileSystem.get(conf)
    val raf = new NcHdfsRaf(fs.getFileStatus(new Path(path)), fs.getConf)
    val netcdfFile = NetcdfFile.open(raf, path.toString)
    val varargs = netcdfFile.getVariables
    println(varargs.get(100).getFullName)
    val variable = netcdfFile.findVariable("mod08/Data_Fields/Aerosol_Optical_Depth_Small_Ocean_Standard_Deviation")
    val array = variable.read()
    ArraySerializer.factory(array)
  }
}
