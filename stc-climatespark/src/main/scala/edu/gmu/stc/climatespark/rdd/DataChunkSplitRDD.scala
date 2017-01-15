package edu.gmu.stc.climatespark.rdd

import java.io.{BufferedReader, InputStreamReader}
import java.util.HashMap

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import edu.gmu.stc.climatespark.util.StringUtils
import edu.gmu.stc.hadoop.raster._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SerializableWritable, SparkContext, TaskContext}

import scala.collection.JavaConverters._

/**
  * Created by Fei Hu on 1/9/17.
  */

class DataChunkSplitPartition(@transient dataChunkScheduler: (DataChunkHosts, Array[DataChunkCorner]),
                              val index: Int) extends Partition {
  val dataChunkPartitions = dataChunkScheduler
}

/**
  *
  * @param dataChunkCoordScheduled _1 is the host location, _2 is the corner list for the chunks located at the hosts
  * @param sc
  * @param conf
  */
class DataChunkSplitRDD(
                         dataChunkCoordScheduled: Array[(DataChunkHosts, Array[DataChunkCorner])],
                         dataChunkMeta: DataChunkMeta,
                         sc: SparkContext,
                         @transient conf: Configuration)
                    extends RDD[InputSplit](sc, Nil){

  private val confBroadcast = sc.broadcast(new SerializableWritable(conf))

  private val dataChunkMetaBroadcast = sc.broadcast(dataChunkMeta)


  def getConf: Configuration = {
    val conf: Configuration = confBroadcast.value.value
    conf
  }

  def getDataChunkMeta: DataChunkMeta = {
    val dataChunkMeta: DataChunkMeta = dataChunkMetaBroadcast.value
    dataChunkMeta
  }

  def getDataNodeIndex(nodeIndexFile: Path): HashMap[java.lang.Double, DataChunkByteLocation] = {
    val fs = FileSystem.get(this.getConf)
    val inputStream = fs.open(nodeIndexFile)
    val br = new BufferedReader(new InputStreamReader(inputStream))
    val gson = new Gson()
    val hashMap = gson.fromJson(br, new TypeToken[HashMap[java.lang.Double, DataChunkByteLocation]](){}.getType).asInstanceOf[HashMap[java.lang.Double, DataChunkByteLocation]]
    hashMap
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[InputSplit] = {
    val dataChunkCoords = split.asInstanceOf[DataChunkSplitPartition].dataChunkPartitions
    val hosts = dataChunkCoords._1.getHosts
    val hostID = StringUtils.getHostID(hosts(0)) //StringUtils.getHostID(InetAddress.getLocalHost.getHostName)
    val nodeIndexFile = new Path("/Users/feihu/Documents/Data/Merra2/index/nodeindex/preccu_" + hostID + ".txt")
    val nodeIndex = getDataNodeIndex(nodeIndexFile)

    val dataChunkMeta = getDataChunkMeta

    val dataChunks = new Array[DataChunk](dataChunkCoords._2.size)

    for (i <- dataChunkCoords._2.indices) {
      val dataChunkCorner = dataChunkCoords._2(i)
      val dataChunkByteLocation = nodeIndex.get(dataChunkCorner.getID)
      val corner = dataChunkCorner.getCorner
      val filePath = dataChunkMeta.getFilePathPattern.replace("*", corner(0).toString)

      val shape = dataChunkMeta.getShape.slice(1, dataChunkMeta.getShape.size)
      val dims = dataChunkMeta.getDimensions.slice(1, dataChunkMeta.getDimensions.size)

      dataChunks(i) = new DataChunk(corner.slice(1, corner.size), shape, dims,
        dataChunkByteLocation.getFilePos, dataChunkByteLocation.getByteSize,
        dataChunkMeta.getFilterMask, hosts, dataChunkMeta.getDataType, dataChunkMeta.getVarShortName, filePath)
    }

    val results = new Array[InputSplit](1)
    results(0) = new DataChunkInputSplit(dataChunks.toList.asJava)
    results.toIterator
  }

  override protected def getPartitions: Array[Partition] = {
    val partitions = new Array[Partition](dataChunkCoordScheduled.size)
    for (i <- 0 until dataChunkCoordScheduled.size) {
      partitions(i) = new DataChunkSplitPartition(dataChunkCoordScheduled(i), i)
    }
    partitions
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[DataChunkSplitPartition].dataChunkPartitions._1.getHosts
  }
}
