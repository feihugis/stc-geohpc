package edu.gmu.stc.climatespark.test


import org.apache.spark.SparkContext

import com.intel.analytics.bigdl.utils.Engine

/**
  * Created by Fei Hu on 4/21/17.
  */
object BigDL {

  def main(args: Array[String]): Unit = {
    val conf = Engine.createSparkConf().setMaster("local[4]")
      .setAppName("Train Lenet on MNIST")
      .set("spark.task.maxFailures", "1")
      .set("bigdl.disableCheckSysEnv", "true")

    val sc = new SparkContext(conf)
    Engine.init
  }

}
