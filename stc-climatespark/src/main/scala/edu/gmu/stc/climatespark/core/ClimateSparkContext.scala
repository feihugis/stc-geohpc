package edu.gmu.stc.climatespark.core

import edu.gmu.stc.climatespark.io.ClimateSparkKryoRegistrator
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer
import edu.gmu.stc.hadoop.raster.{DataChunkInputFormat, DataChunk}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Fei Hu on 11/15/16.
  */
class ClimateSparkContext (@transient val sparkContext: SparkContext){
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  val hConf = new Configuration()
  val dataChunk = classOf[edu.gmu.stc.hadoop.raster.DataChunk]
  val arraySerializer = classOf[edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer]
  val inputFormat = classOf[DataChunkInputFormat].asInstanceOf[Class[F] forSome {type F <: InputFormat[DataChunk, ArraySerializer]}]

  def this(conf: SparkConf) {
    this(new SparkContext(conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                .set("spark.kryo.registrator", classOf[ClimateSparkKryoRegistrator].getName)))
  }

  def this(sc: SparkContext, hadoopConf: String) {
    this(sc)
    this.hConf.addResource(new Path(hadoopConf))
    this.sparkContext.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                             .set("spark.kryo.registrator", classOf[ClimateSparkKryoRegistrator].getName)
  }

  def this(sparkConf: SparkConf, hadoopConf: String) {
    this(new SparkContext(sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                   .set("spark.kryo.registrator", classOf[ClimateSparkKryoRegistrator].getName)))
    this.hConf.addResource(new Path(hadoopConf))
  }

  def this(hadoopConf: String, uri: String, appName: String) {
    this(new SparkContext(new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[ClimateSparkKryoRegistrator].getName)
      .setMaster(uri)
      .setAppName(appName)))
    this.hConf.addResource(new Path(hadoopConf))
  }

  def this(hadoopConf: Configuration, uri: String, appName: String) {
    this(new SparkContext(new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[ClimateSparkKryoRegistrator].getName)
      .setMaster(uri)
      .setAppName(appName)))
    this.hConf.addResource(hadoopConf)
  }

  def getClimateRDD: RDD[(DataChunk, ArraySerializer)] = {
    this.sparkContext.newAPIHadoopRDD(this.hConf, inputFormat, dataChunk, arraySerializer).map(rdd => rdd ).filter(_._1 != null)
  }

  def getHadoopConfig = this.hConf

  def getSparkContext = this.sparkContext
}
