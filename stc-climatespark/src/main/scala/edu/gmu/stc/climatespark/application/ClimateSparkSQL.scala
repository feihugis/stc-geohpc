package edu.gmu.stc.climatespark.application

import edu.gmu.stc.climatespark.core.ClimateSparkContext
import edu.gmu.stc.climatespark.functions.ClimateRDDFunctions._
import edu.gmu.stc.climatespark.util.RuntimeMonitor
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Fei Hu on 2/9/17.
  */
object ClimateSparkSQL {

  def main(args: Array[String]) {
    val sc = new SparkContext()
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.FATAL)

    val variable = sqlContext.read.parquet("/parquet/preccu_merra2_1980_1996.parquet")
    variable.registerTempTable("merra")
    sqlContext.sql("select * from merra where time >= 19860101 and time <= 19901231").registerTempTable("merra2")

    RuntimeMonitor.show_multi_runtiming(
      sqlContext.sql("select avg(value) from merra where time >= 19850101 and time <= 19851231 and lat <= 41.001 and lat >= 37.0077 and lon <= -102.040513 and lon >= -109.071 and hour >= 0 and hour <= 24 group by time").collect(), 1)

    sqlContext.cacheTable("merra2")
    //val sqlLang = "select avg(value) from merra2 where lat <= 91 and lon <= 288 and time >= 19860101 and time <= 19861231 and hour >= 0 and hour <=2 group by lat, lon"
    //val sqlLang = "select avg(value) from merra2 where ((lat <= 273 and lon <= 576) or (lat <= 364 and lat >= 273 and lon <= 288)) and time >= 19860101 and time <= 19901231 and hour >= 0 and hour <=2 group by lat, lon"

    var sqlLang = "select * from merra2 where lat = 89.5 and lon = -174.5 and time = 19880814 and hour = 20"
    RuntimeMonitor.show_multi_runtiming(sqlContext.sql(sqlLang).count(), 1)

    sqlLang = "select * from merra2 where lat <= 89.5 and lat >= 89.5 and lon <= -174.5 and lon >= -174.5 and time >= 19860101 and time <= 19860131 and hour >= 0 and hour <=2"
    RuntimeMonitor.show_multi_runtiming(sqlContext.sql(sqlLang).count(), 3)

    sqlLang = "select (time - 19860101)*(time - 19860101) + (hour - 0)*(hour - 0) + (lat - 0)*(lat - 0) + (lon - 0)*(lon - 0) as dist from merra2"

    RuntimeMonitor.show_multi_runtiming(sqlContext.sql(sqlLang).sort("dist").show(), 1)








    val configFile = "/var/lib/hadoop-hdfs/0130/merra2-climatespark-config.xml"
    val climateSparkSC = new ClimateSparkContext(sc, configFile)
    val climateRDD = climateSparkSC.getClimateRDD
    val cellRDD = climateRDD.query3DPointTimeSeries
    val df = cellRDD.toDF()
    df.registerTempTable("merra2")
    sqlContext.sql("select * from merra2 where time > 19810101").show(100)
    sqlContext.sql("select * from merra2 where time >= 19951231").saveAsParquetFile("/parquet/preccu_merra2_1980_1996.parquet")

    df.saveAsParquetFile("/parquet/preccu_merra2_1980_1996.parquet")

  }

}
