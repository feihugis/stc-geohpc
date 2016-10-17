package edu.gmu.stc.hadoop.raster.io.datastructure;

import com.esotericsoftware.kryo.KryoSerializable;

import org.apache.hadoop.io.Writable;

import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import ucar.ma2.ArrayInt;
import ucar.ma2.ArrayShort;

/**
 * Created by Fei Hu on 8/26/16.
 */
public abstract class ArraySerializer implements KryoSerializable, Writable {

  public abstract Array getArray();
  public abstract void setArray(Array array);

  public static ArraySerializer factory(Array array) {
    if (array instanceof ArrayFloat) {
      ArrayFloatSerializer arrayFloatSerializer = new ArrayFloatSerializer();
      arrayFloatSerializer.setArray(array);
      return arrayFloatSerializer;
    }

    if (array instanceof ArrayInt) {
      ArrayIntSerializer arrayIntSerializer = new ArrayIntSerializer();
      arrayIntSerializer.setArray(array);
      return arrayIntSerializer;
    }

    if ( array instanceof ArrayShort) {
      ArrayShortSerializer arrayShortSerializer = new ArrayShortSerializer();
      arrayShortSerializer.setArray(array);
      return arrayShortSerializer;
    }

    return null;
  }

}
