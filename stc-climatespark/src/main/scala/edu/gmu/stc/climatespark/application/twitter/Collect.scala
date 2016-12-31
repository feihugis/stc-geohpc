package edu.gmu.stc.climatespark.application.twitter

import com.google.gson.Gson
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.twitter.TwitterUtils

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Fei Hu on 12/24/16.
  */
object Collect {
  private var numTweetsCollected = 0L
  private var partNum = 0
  private var gson = new Gson()

  def main(args: Array[String]) {
    // Process program arguments and set properties
    val inputArgs = Array("/Users/feihu/Desktop/twitter/test", "10000", "10", "2")
    if (args.length < 3) {
      System.err.println("Usage: " + this.getClass.getSimpleName +
        "<outputDirectory> <numTweetsToCollect> <intervalInSeconds> <partitionsEachInterval>")
      System.exit(1)
    }

    val Array(outputDirectory, Utils.IntParam(numTweetsToCollect),
    Utils.IntParam(intervalSecs),
    Utils.IntParam(partitionsEachInterval)) = Utils.parseCommandLineWithTwitterCredentials(args)

    /*val outputDir = new File(outputDirectory.toString)
    if (outputDir.exists()) {
      System.err.println("ERROR - %s already exists: delete or specify another directory".format(outputDirectory))
      System.exit(1)
    }

    outputDir.mkdirs()*/

    println("Initializing Streaming Spark Context...")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)/*.setMaster("local[6]")*/
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val ssc = new StreamingContext(sc, Seconds(intervalSecs))
    //val filter = Array("Zika", "zika", "ZIKA", "ZikaVirus")
    //val tweetStream = TwitterUtils.createStream(ssc, Utils.getAuth, filter).map(gson.toJson(_))

    val tweetStream = TwitterUtils.createStream(ssc, Utils.getAuth).filter(
      status => {
        if (status.getGeoLocation != null) {
          true
        } else {
          false
        }
      }
    ).map(gson.toJson(_))

    tweetStream.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.repartition(partitionsEachInterval)
        outputRDD.saveAsTextFile(outputDirectory + "/tweets_" + time.milliseconds.toString)
        numTweetsCollected += count
        //if (numTweetsCollected > numTweetsToCollect) {
        //  System.exit(0)
        //}
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
