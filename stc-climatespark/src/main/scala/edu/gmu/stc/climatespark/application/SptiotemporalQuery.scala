package edu.gmu.stc.climatespark.application

import edu.gmu.stc.climatespark.core.ClimateSparkContext
import edu.gmu.stc.climatespark.functions.ClimateRDDFunctions._

/**
  * Created by Fei Hu on 11/15/16.
  */


object SptiotemporalQuery {

  def main(args: Array[String]) {
    val configFile = "/Users/feihu/Documents/GitHub/stc-geohpc/stc-spark/src/main/resources/mod08-climatespark-config.xml"
    val sc = new ClimateSparkContext(configFile, "local[6]", "test")

    val climateRDD = sc.getClimateRDD
    println(climateRDD.collect().size)

    val cellRDD = climateRDD.queryPointTimeSeries

    val avgRDD = climateRDD.avgDaily

    val timeAvgRDD = climateRDD.timeAvg(1)

    println(climateRDD.count())

    //println(avgRDD.foreach(cell => println(cell)))
    //cellRDD.foreach(cell => println(cell))
    println(timeAvgRDD.foreach(rdd => println(rdd._2.getArray.getSize)))
  }
}
