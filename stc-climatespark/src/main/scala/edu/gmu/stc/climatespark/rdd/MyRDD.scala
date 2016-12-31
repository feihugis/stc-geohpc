package edu.gmu.stc.climatespark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark._

import scala.annotation.meta.param

/**
  * Created by Fei Hu on 12/29/16.
  */
class MyRDD(
             sc: SparkContext,
             numPartitions: Int,
             dependencies: List[Dependency[_]],
             locations: Seq[Seq[String]] = Nil)
  extends RDD[(Int, Int)](sc, dependencies) with Serializable {

  override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] =
    throw new RuntimeException("should not be reached")

  override def getPartitions: Array[Partition] = (0 until numPartitions).map(i => new Partition {
    override def index: Int = i
  }).toArray

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    if (locations.isDefinedAt(partition.index)) {
      locations(partition.index)
    } else {
      Nil
    }
  }

  override def toString: String = "DAGSchedulerSuiteRDD " + id
}
