package edu.gmu.stc.climatespark.application

import edu.gmu.stc.climatespark.core.ClimateSparkContext
import org.apache.spark.sql.SQLContext
import edu.gmu.stc.climatespark.functions.GenericFileRDDFunctions._
import edu.gmu.stc.climatespark.io.ClimateSparkKryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Fei Hu on 4/3/17.
  */
object NetCDFBuildIndex {

  def main(args: Array[String]): Unit = {
    val configFile = "/Users/feihu/Documents/GitHub/stc-geohpc/stc-spark/src/main/resources/merra2-climatespark-config.xml"

    val sc = new SparkContext(new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[ClimateSparkKryoRegistrator].getName)
      .setMaster("local[6]")
      .setAppName("test")
      .set("spark.driver.allowMultipleContexts", "true"))

    val climateSparkSC = new ClimateSparkContext(sc, configFile)

    val genericFileRDD = climateSparkSC.getGenericFileRDD
    val varShortName = "T2M"
    val timePattern = """\d\d\d\d\d\d\d"""
    val fullVarName = "T2M"

    val filteredFileRDDByTime = genericFileRDD.filterFileByTime(timePattern, 20160101, 20161231)

    val message = filteredFileRDDByTime.buildIndex(climateSparkSC.getHadoopConfig)

    println(message)
  }

}
