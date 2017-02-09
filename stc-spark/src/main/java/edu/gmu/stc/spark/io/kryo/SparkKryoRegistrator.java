package edu.gmu.stc.spark.io.kryo;

import com.esotericsoftware.kryo.Kryo;

import org.apache.spark.serializer.KryoRegistrator;

import edu.gmu.stc.hadoop.index.kdtree.KDTree;
import edu.gmu.stc.hadoop.raster.index.DataChunkIndexBuilderImp;
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer;
import edu.gmu.stc.hadoop.raster.io.datastructure.ArrayShortSerializer;

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

      kryo.register(Class.forName("edu.gmu.stc.hadoop.raster.io.datastructure.ArrayFloatSerializer"));
      kryo.register(Class.forName("edu.gmu.stc.hadoop.raster.io.datastructure.ArrayIntSerializer"));
      kryo.register(ArrayShortSerializer.class);
      kryo.register(ArraySerializer.class);

      kryo.register(Class.forName("edu.gmu.stc.hadoop.raster.DataChunk"));
      kryo.register(Class.forName("edu.gmu.stc.hadoop.raster.hdf5.H5Chunk"));
      kryo.register(Class.forName("ucar.ma2.ArrayInt"));
      kryo.register(Class.forName("edu.gmu.stc.hadoop.raster.hdf5.Merra2Chunk"));
      //kryo.register(Class.forName("edu.gmu.stc.hadoop.raster.index.DataChunkIndexBuilderImp"));
      kryo.register(DataChunkIndexBuilderImp.class);
      kryo.register(KDTree.class);

    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
}
