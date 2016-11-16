package edu.gmu.stc.climatespark.io

import com.esotericsoftware.kryo.Kryo
import edu.gmu.stc.hadoop.raster.DataChunk
import edu.gmu.stc.hadoop.raster.io.datastructure.{ArraySerializer, ArrayShortSerializer, ArrayFloatSerializer}
import org.apache.spark.serializer.KryoRegistrator

/**
  * Created by Fei Hu on 11/15/16.
  */
class ClimateSparkKryoRegistrator extends KryoRegistrator{
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[ArrayFloatSerializer]);
    kryo.register(classOf[ArrayShortSerializer]);
    kryo.register(classOf[ArraySerializer]);
    kryo.register(classOf[DataChunk]);
    kryo.register(classOf[ArraySerializer]);
    kryo.register(classOf[scala.Tuple2[DataChunk,ArrayFloatSerializer]]);
    //kryo.register(classOf[Cell])
  }
}
