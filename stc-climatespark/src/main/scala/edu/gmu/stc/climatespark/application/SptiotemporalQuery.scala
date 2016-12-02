package edu.gmu.stc.climatespark.application

import edu.gmu.stc.climatespark.core.ClimateSparkContext
import edu.gmu.stc.climatespark.functions.ClimateRDDFunctions._
import edu.gmu.stc.climatespark.io.datastructure.{MultiCell3, MultiCell}
import edu.gmu.stc.hadoop.commons.ClimateHadoopConfigParameter
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

/**
  * Created by Fei Hu on 11/15/16.
  */


object SptiotemporalQuery {

  def main(args: Array[String]) {
    val configFile = "/Users/feihu/Documents/GitHub/stc-geohpc/stc-spark/src/main/resources/mod08-climatespark-config.xml"
    val sc = new ClimateSparkContext(configFile, "local[6]", "test")
    val sqlContext = new SQLContext(sc.getSparkContext)
    import sqlContext.implicits._

    val climateRDD_1 = sc.getClimateRDD

    val weightedAreaAvg = climateRDD_1.weightedAreaAvgDaily

    weightedAreaAvg.foreach(rdd => print(rdd + "\n"))

    val cellRDD_1 = climateRDD_1.queryPointTimeSeries

    cellRDD_1.foreach(cell => {
      if (cell.value != -9999) {
        print(cell + "\n")
      }
    })

    val mulCellRDD = climateRDD_1.joinVars.map(u => u.asInstanceOf[MultiCell3])

    val varNames = sc.getHadoopConfig.get(ClimateHadoopConfigParameter.QUERY_VARIABLE_NAMES).split(",").toList

    val schema = List("lat","lon","time"):::varNames
    mulCellRDD.toDF(schema(0), schema(1), schema(2), schema(3), schema(4), schema(5)).saveAsParquetFile("/Users/feihu/Desktop/mulcell")

    val df = sqlContext.createDataFrame(mulCellRDD)

    df.registerTempTable("multiVars")

    //df.toDF().write.format("com.databricks.spark.csv").save("/Users/feihu/Desktop/mulcell.csv")



    //mulCellRDD.saveAsObjectFile("Users/feihu/Desktop/mulcell.csv")
    //mulCellRDD.toDF.registerTempTable("multiVars")

    sqlContext.sql("Select * From multiVars").show()


    val avgRDD = climateRDD_1.avgDaily
    val timeAvgRDD = climateRDD_1.timeAvg(1)

    sc.getHadoopConfig.set(ClimateHadoopConfigParameter.QUERY_VARIABLE_NAMES, "Sensor_Azimuth_Mean")
    val climateRDD_2 = sc.getClimateRDD
    val cellRDD_2 = climateRDD_2.queryPointTimeSeries

    sc.getHadoopConfig.set(ClimateHadoopConfigParameter.QUERY_VARIABLE_NAMES, "Aerosol_Scattering_Angle_Mean")
    val climateRDD_3 = sc.getClimateRDD
    val cellRDD_3 = climateRDD_3.queryPointTimeSeries



   // cellRDD_1.toDF().registerTempTable("var1")
   // cellRDD_2.toDF().registerTempTable("var2")
    //cellRDD_3.toDF().registerTempTable("var3")

   /* sqlContext.sql("Select var1.lat, var1.lon, var2.time, var1.value as A, var2.value as B " +
      "From var1, var2 " +
      "Where var1.lat = var2.lat and var1.lon = var2.lon and var1.time = var2.time").registerTempTable("var_1_2")*/


    //println(climateRDD.count())

    //println(avgRDD.foreach(cell => println(cell)))
    //cellRDD.foreach(cell => println(cell))
   // println(timeAvgRDD.foreach(rdd => println(rdd._2.getArray.getSize)))


    //cellRDD.toDF().write.format("com.databricks.spark.csv").save("/Users/feihu/Desktop/cell.csv")


  }
}
