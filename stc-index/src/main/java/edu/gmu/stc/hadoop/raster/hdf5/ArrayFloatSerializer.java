package edu.gmu.stc.hadoop.raster.hdf5;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.apache.hadoop.io.Writable;
import org.apache.spark.serializer.KryoSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import ucar.ma2.ArrayInt;

/**
 * Created by Fei Hu on 3/9/16.
 */
public class ArrayFloatSerializer implements Writable, KryoSerializable {
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

  @Override
  public void write(Kryo kryo, Output output) {
    float[] storage = (float[]) array.getStorage();
    int[] shape = array.getShape();

    output.writeInt(shape.length);
    for (int i=0; i<shape.length; i++) {
      output.writeInt(shape[i]);
    }

    output.writeInt(storage.length);
    for (int i = 0; i<storage.length; i++) {
      output.writeFloat(storage[i]);
    }
  }

  @Override
  public void read(Kryo kryo, Input input) {
    int shapeSize = input.readInt();
    int[] shape = new int[shapeSize];
    for (int i=0; i<shapeSize; i++) {
      shape[i] = input.readInt();
    }
    int dataSize = input.readInt();
    float[] data = new float[dataSize];
    for (int i=0; i<dataSize; i++) {
      data[i] = input.readFloat();
    }
    array = (ArrayFloat) Array.factory(float.class, shape, data);
  }
}
