package edu.gmu.stc.climatespark.rdd

import edu.gmu.stc.climatespark.rdd.indexedrdd.IndexedRDD
import edu.gmu.stc.climatespark.rdd.indexedrdd.IndexedRDD._
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer
import edu.gmu.stc.hadoop.raster.{DataChunk, DataChunkReader}
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import org.apache.spark.storage.StorageLevel



class DataChunkRDD(partitionsRDD: RDD[DataChunkPartition],
                   numPartitions: Int,
                   locations: IndexedRDD[Int, Array[String]],
                   sc: SparkContext,
                   conf: Configuration)
    extends RDD[(DataChunk, ArraySerializer)](sc, partitionsRDD.dependencies){


  private val confBroadcast = sc.broadcast(new SerializableWritable(conf))


  def getConf: Configuration = {
    val conf: Configuration = confBroadcast.value.value
    conf
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(DataChunk, ArraySerializer)] = {
    val dataChunkPartitionArray = partitionsRDD.iterator(split, context).toArray
    val size = dataChunkPartitionArray.size
    if (size != 0) {
      val conf = getConf
      /*var kvNum = 0
      for (i <- 0 until size) {
        kvNum = kvNum + dataChunkPartitionArray(i).serializableDataChunk.value.getLength.toInt
      }*/
      //var results = new Array[(DataChunk, ArraySerializer)](kvNum)
      //var count = 0
      val results = new ArrayBuffer[(DataChunk, ArraySerializer)]

      for (i <- 0 until size) {
        val inputSplit = dataChunkPartitionArray(i).serializableDataChunk.value
        val recordReader = new DataChunkReader()
        recordReader.initialize(inputSplit, conf)

        while (recordReader.nextKeyValue()) {
          val key = recordReader.getCurrentKey
          val value = recordReader.getCurrentValue
          if (key != null) {
            val r = (key, value)
            results += r
            //count = count + 1
          }
        }
      }

      results.toIterator
      //new InterruptibleIterator[(DataChunk, ArraySerializer)](context, iter)
    } else {
      val tt = new Array[(DataChunk, ArraySerializer)](0)
      tt.toIterator
    }
  }


  override protected def getPartitions: Array[Partition] = {
    //partitionsRDD.toLocalIterator.toArray
    //val numPartitions = partitionsRDD.partitions.size
    //partitionsRDD.partitions

    /*(1 until numPartitions).map(i => new Partition {
      override def index: Int = i
    }).toArray*/
    //this.sc.statusTracker.getStageInfo()
    //val partitions = firstParent[DataChunkPartition].partitions
    //partitions
    (0 until numPartitions).map(i => new Partition {
      override def index: Int = i
    }).toArray

    /*
    val datachunkPartitions = partitionsRDD.collect()
    val partitions = new Array[Partition](datachunkPartitions.size)
    for (i <- 0 until datachunkPartitions.size) {
      partitions(i) = datachunkPartitions(i)
    }
    partitions*/
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    //val hosts = split.asInstanceOf[DataChunkPartition].serializableDataChunk.value.getLocations
    //val hosts = firstParent[DataChunkPartition].first().serializableDataChunk.value.getLocations
    //val hosts = Array("abc", "dce")
    //println(hosts.toArray.deep.mkString(","))

    //val hosts = locations(split.index).toSeq
    //hosts
    //val hosts = locations.lookup(split.index).head
    //hosts
    //val hosts = locations.get(split.index)
    //hosts.head

    val test = locations.lookup(0)

    Array("abc", "dce").toSeq
  }

  def get(split: Partition) = {
    val partitions = new Array[Int](1)
    partitions(0) = split.index
    val results: Array[Array[DataChunkPartition]] = context.runJob(
      partitionsRDD, (context: TaskContext, partIter: Iterator[DataChunkPartition]) => partIter.toArray, partitions, allowLocal = true)

    val num = results.size
  }
}

object DataChunkRDD {

  def apply(sc: SparkContext, conf: Configuration,
            elems: RDD[DataChunk], groupType: String):
                                            DataChunkRDD = updatable(sc, conf, elems, groupType)

  def updatable(sc: SparkContext, conf: Configuration,
                elems: RDD[DataChunk], groupType: String): DataChunkRDD = {
    val groupRDD = elems.map(groupByFileAndHosts)
                        .reduceByKey(_ ++ _).cache()
                        .zipWithIndex()
                        .map(d => (d._2, d._1._2)).cache()
                        /*.partitionBy(new DataChunkPartitioner(123))
                        .map(d => new DataChunkPartition(d._1.hashCode(), d._2))
                        .cache()
*/
    val partNum = groupRDD.count().toInt
    //val hosts = groupRDD.values.map(d => d(0).getHosts).collect().toSeq

    val partitions = groupRDD.partitionBy(new DataChunkPartitioner(partNum))
                             .map(d => new DataChunkPartition(d._1.hashCode, d._2))
    partitions.cache()


    /*val hosts = partitions.map(d => d.serializableDataChunk.value.getLocations).collect().toSeq
    new DataChunkRDD(partitions, hosts.size, hosts, sc, conf)*/
    val hosts = partitions.map(d => (d.index, d.serializableDataChunk.value.getLocations))
    hosts.foreach(d => println(d._1 + " ------ " + d._2))
    val indexHosts = IndexedRDD(hosts).cache()
    indexHosts.foreach(d => println(d._1, d._2))

    val cc = indexHosts.get(1).toArray
    new DataChunkRDD(partitions, indexHosts.count().toInt, indexHosts, sc, conf)


      /*
      mapPartitionsWithIndex[DataChunkPartition](
      (index, iter) => {
        val size = iter.size
        val array = new Array[DataChunkPartition](size)
        val count = 0;
        iter.foreach(tuple => {
          array(count) = new DataChunkPartition(id, index, tuple._2)
        })
        array.toIterator
      }, preservesPartitioning = true)
      */
  }

  def groupByFileAndHosts(elem: DataChunk): Tuple2[String, Array[DataChunk]] = {
    val value = new Array[DataChunk](1)
    value(0) = elem
    //here assume that the time corresponds to a file
    (elem.queryDataLocations + elem.getTime, value)
  }
}



