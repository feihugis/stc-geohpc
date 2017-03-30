package edu.gmu.stc.climatespark.io

import com.esotericsoftware.kryo.Kryo
import edu.gmu.stc.climatespark.io.datastructure.Cell
import edu.gmu.stc.hadoop.index.kdtree.{HPoint, KDNode, KDTree}
import edu.gmu.stc.hadoop.raster.DataChunk
import edu.gmu.stc.hadoop.raster.io.datastructure.{ArrayFloatSerializer, ArraySerializer, ArrayShortSerializer}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.serializer.KryoRegistrator

/**
  * Created by Fei Hu on 11/15/16.
  */
class ClimateSparkKryoRegistrator extends KryoRegistrator{
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[ArrayFloatSerializer])
    kryo.register(classOf[ArrayShortSerializer])
    kryo.register(classOf[ArraySerializer])
    kryo.register(classOf[DataChunk])
    kryo.register(classOf[ArraySerializer])
    kryo.register(classOf[scala.Tuple2[DataChunk, ArrayFloatSerializer]])
    kryo.register(classOf[Cell])
    kryo.register(classOf[KDTree[ArrayFloatSerializer]])
    kryo.register(classOf[HPoint])
    kryo.register(classOf[KDNode[ArrayFloatSerializer]])
    kryo.register(classOf[Text])
    kryo.register(classOf[FileSplit])

    //kryo.register(scala.Tuple2[].class)
    //kryo.register(classOf[scala.Tuple2[]])
    //kryo.register(classOf[Cell])
  }
}
