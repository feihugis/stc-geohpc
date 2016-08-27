package edu.gmu.stc.hadoop.raster.io.datastructure;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import ucar.ma2.ArrayInt;

/**
 * Created by Fei Hu on 3/13/16.
 */
public class ArrayIntSerializer extends ArraySerializer {
  ArrayInt array;

  public ArrayIntSerializer() {

  }

  public ArrayIntSerializer(ArrayInt arrayInt) {
    this.array = arrayInt;
  }

  public ArrayIntSerializer(int[] shape, int[] data) {
    array = (ArrayInt) Array.factory(int.class, shape, data);
  }

  public ArrayInt getArray() {
    return array;
  }

  @Override
  public void setArray(Array array) {
    this.array = (ArrayInt) array;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    int[] storage = (int[]) array.getStorage();
    int[] shape = array.getShape();

    out.writeInt(shape.length);
    for (int i=0; i<shape.length; i++) {
      out.writeInt(shape[i]);
    }

    out.writeInt(storage.length);
    for (int i = 0; i<storage.length; i++) {
      out.writeInt(storage[i]);
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
    int[] data = new int[dataSize];
    for (int i=0; i<dataSize; i++) {
      data[i] = in.readInt();
    }

    array = (ArrayInt) Array.factory(int.class, shape, data);
  }

  @Override
  public void write(Kryo kryo, Output output) {
    int[] storage = (int[]) array.getStorage();
    int[] shape = array.getShape();

    output.writeInt(shape.length);
    for (int i=0; i<shape.length; i++) {
      output.writeInt(shape[i]);
    }

    output.writeInt(storage.length);
    for (int i = 0; i<storage.length; i++) {
      output.writeInt(storage[i]);
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
    int[] data = new int[dataSize];
    for (int i=0; i<dataSize; i++) {
      data[i] = input.readInt();
    }
    array = (ArrayInt) Array.factory(int.class, shape, data);
  }
}
