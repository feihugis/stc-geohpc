package edu.gmu.stc.spark.io.kryo;

import com.esotericsoftware.kryo.Kryo;

import org.apache.spark.serializer.KryoRegistrator;

/**
 * Created by Fei Hu on 3/17/16.
 */
public class SparkKryoRegistrator implements KryoRegistrator {

  @Override
  public void registerClasses(Kryo kryo) {
    try {
      kryo.register(Class.forName("edu.gmu.stc.spark.io.etl.GeoExtracting"));
      kryo.register(Class.forName("org.apache.hadoop.io.Writable"));
      kryo.register(Class.forName("edu.gmu.stc.hadoop.vector.Polygon"));
      kryo.register(Class.forName("org.apache.hadoop.io.LongWritable"));
      kryo.register(Class.forName("edu.gmu.stc.hadoop.raster.hdf5.ArrayFloatSerializer"));
      kryo.register(Class.forName("edu.gmu.stc.hadoop.raster.hdf5.ArrayIntSerializer"));
      kryo.register(Class.forName("edu.gmu.stc.hadoop.raster.DataChunk"));
      kryo.register(Class.forName("edu.gmu.stc.hadoop.raster.hdf5.H5Chunk"));
      kryo.register(Class.forName("ucar.ma2.ArrayInt"));
      kryo.register(Class.forName("edu.gmu.stc.hadoop.raster.hdf5.Merra2Chunk"));

    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
}
