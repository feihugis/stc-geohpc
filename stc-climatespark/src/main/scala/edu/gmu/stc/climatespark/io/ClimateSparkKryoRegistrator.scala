package edu.gmu.stc.climatespark.io

import com.esotericsoftware.kryo.Kryo
import edu.gmu.stc.climatespark.io.datastructure.{ArrayBbox2D, Cell3D, Cell4D, Cell5D}
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
    kryo.register(classOf[Cell3D])
    kryo.register(classOf[Cell4D])
    kryo.register(classOf[Cell5D])
    kryo.register(classOf[KDTree[ArrayFloatSerializer]])
    kryo.register(classOf[HPoint])
    kryo.register(classOf[KDNode[ArrayFloatSerializer]])
    kryo.register(classOf[Text])
    kryo.register(classOf[FileSplit])
    kryo.register(classOf[ArrayBbox2D])
    kryo.register(classOf[ArrayBbox2D])
    kryo.register(classOf[String])
  }
}
