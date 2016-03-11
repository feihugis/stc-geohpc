package edu.gmu.stc.hadoop.raster.hdf5;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;

/**
 * Created by Fei Hu on 3/9/16.
 */
public class ArrayFloatSerializer implements Writable {
  ArrayFloat array;

  public ArrayFloatSerializer(int[] shape, float[] data) {
    array = (ArrayFloat) Array.factory(float.class, shape, data);
  }

  public ArrayFloat getArray() {
    return array;
  }

  public void setArray(ArrayFloat array) {
    this.array = array;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    float[] storage = (float[]) array.getStorage();
    int[] shape = array.getShape();

    out.writeInt(shape.length);
    for (int i=0; i<shape.length; i++) {
      out.writeInt(shape[i]);
    }

    out.writeInt(storage.length);
    for (int i = 0; i<storage.length; i++) {
      out.writeFloat(storage[i]);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int shapeSize = in.readInt();
    int[] shape = new int[shapeSize];
    for (int i=0; i<shapeSize; i++) {
      shape[i] = in.readInt();
    }
    int dataSize = in.readInt();
    float[] data = new float[dataSize];
    for (int i=0; i<dataSize; i++) {
      data[i] = in.readFloat();
    }
  }
}
