package edu.gmu.stc.climatespark.application

import edu.gmu.stc.climatespark.core.ClimateSparkContext
import edu.gmu.stc.climatespark.functions.DataFormatFunctions
import edu.gmu.stc.climatespark.rdd.DataChunkRDD
import org.apache.spark.SparkConf


/**
  * Created by Fei Hu on 12/26/16.
  */
object IndexedRDDTest {

  /**
    *
    * @param args
    */
  def main(args: Array[String]) {
    val configFile = "/Users/feihu/Documents/GitHub/stc-geohpc/stc-spark/src/main/resources/mod08-climatespark-config.xml"
    val sc = new ClimateSparkContext(configFile, "local[6]", "test")
    //val sparkConf = new SparkConf().setMaster("local[6]").setAppName("test").set("spark.driver.allowMultipleContexts", "true")


    val inputFile = "/Users/feihu/Documents/Data/Merra2/index/preccu"
    val outputFile = "/Users/feihu/Documents/Data/Merra2/index/preccu-index-test-19800101.parquet"
    DataFormatFunctions.csvToParquet(inputFile, outputFile, 100, sc.getSparkContext)

    val dataChunkRDD = DataFormatFunctions.parquetToRDD(sc.getSparkContext, outputFile).filter(d => d.getTime>1980 && d.getTime < 1985)

    //dataChunkRDD.foreach(d => println(d.toString))

    //val num = dataChunkRDD.count()

    val testRDD = DataChunkRDD(sc.getSparkContext, sc.getHadoopConfig, dataChunkRDD, "test").cache()
    testRDD.saveAsTextFile()

   testRDD.count()

   /* testRDD.partitions.foreach(partition => println("index:" + partition.index + "  hasCode:" + partition.hashCode()))
    println("dependency size:" + dataChunkRDD.dependencies)
    println(testRDD)

    println(testRDD.first()._1)

    testRDD.dependencies.foreach { dep =>
             println("dependency type:" + dep.getClass)
             println("dependency RDD:" + dep.rdd)
             println("dependency partitions:" + dep.rdd.partitions)
             println("dependency partitions size:" + dep.rdd.partitions.length)
           }*/


    /*
    println(dataChunkRDD.count())

    val groupRDD = dataChunkRDD.map(datachunk => {
      val key = datachunk.getFilePath + datachunk.getDataLocations
      val value = new Array[DataChunk](1)
      value(0) = datachunk
      (key, value)
    }).reduceByKey((array1, array2) => {
      array1 ++ array2
    })

    groupRDD.partitions.foreach(partition => println("index:" + partition.index + "  hasCode:" + partition.hashCode()))
    println("dependency size:" + dataChunkRDD.dependencies)
    println(dataChunkRDD)

  */

    //print(groupRDD.count())

    //val hostRDD = dataChunkRDD.map(datachunk => (datachunk.getDataLocations, 1)).reduceByKey(_ + _)

    //hostRDD.saveAsTextFile("/Users/feihu/Documents/Data/Merra2/index/dataDist.txt")


    /*
    val df = sqlContext.read.parquet("/Users/feihu/Documents/Data/Merra2/index/preccu-index.parquet")
    df.show()
    val dataChunkRDD = df.map(r => Schema.getDataChunkFromRow(r)).map(chunk => (chunk.getId, chunk))

    val indexedRDD = IndexedRDD(dataChunkRDD).cache()

    //indexedRDD.foreach(r => print(r._1))
    RuntimeMonitor.show_timing(print(indexedRDD.get("PRECCU19850711[11, 0, 288]")))

    val querys = Array("PRECCU19850711[11, 0, 288]", "PRECCU19850711[11, 0, 288]",
      "PRECCU19850711[11, 0, 288]", "PRECCU19850711[11, 0, 288]",
      "PRECCU19850711[11, 0, 288]", "PRECCU19850711[11, 0, 288]")

    RuntimeMonitor.show_timing(indexedRDD.multiget(querys))

    val querys2 = new Array[String](1000)
    for (i <- 0 to 999) {
      querys2(i) = "PRECCU" + (19800101 + i) + "[11, 0, 288]"
    }

    RuntimeMonitor.show_timing(indexedRDD.multiget(querys2))
    */






    //val climateRDD = sc.getClimateRDD.map(rdd => (rdd._1.getId, rdd._1))

    //val indexedRDD = IndexedRDD(rdd).cache()

    //indexedRDD.foreach(r => print(r._1))




/*
    val rdd = sc.getSparkContext.parallelize(1980010101 to 1983123124).map(xml => (xml.toLong, 0))
    val indexed = IndexedRDD(rdd).cache()

    // Perform a point update.
    val indexed2 = indexed.put(1234L, 10873).cache()
    // Perform a point lookup. Note that the original IndexedRDD remains
    // unmodified.
    indexed2.get(1234L) // => Some(10873)
    indexed.get(1234L) // => Some(0)

    val query = 1980010101L to 1981101024L toArray

    RuntimeMonitor.show_timing(indexed2.multiget(query))

    // Efficiently join derived IndexedRDD with original.
    val indexed3 = indexed.innerJoin(indexed2) { (id, a, b) => b }.filter(_._2 != 0)
    indexed3.collect // => Array((1234L, 10873))

    // Perform insertions and deletions.
    val indexed4 = indexed2.put(-100L, 111).delete(Array(998L, 999L)).cache()
    indexed2.get(-100L) // => None
    indexed4.get(-100L) // => Some(111)
    indexed2.get(999L) // => Some(0)
    indexed4.get(999L) // => None
*/


  }
}
