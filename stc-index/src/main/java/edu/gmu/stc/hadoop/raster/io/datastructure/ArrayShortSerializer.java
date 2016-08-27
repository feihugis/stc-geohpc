package edu.gmu.stc.hadoop.raster.io.datastructure;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import ucar.ma2.ArrayShort;

/**
 * Created by Fei Hu on 8/26/16.
 */
public class ArrayShortSerializer extends ArraySerializer{
  private ArrayShort array;

  public ArrayShortSerializer() {

  }

  public ArrayShortSerializer(ArrayShort array) {
    this.array = array;
  }

  @Override
  public void write(Kryo kryo, Output output) {
    short[] storage = (short[]) array.getStorage();
    int[] shape = array.getShape();

    output.writeInt(shape.length);
    for (int i=0; i<shape.length; i++) {
      output.writeInt(shape[i]);
    }

    output.writeInt(storage.length);
    for (int i = 0; i<storage.length; i++) {
      output.writeShort(storage[i]);
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
    short[] data = new short[dataSize];
    for (int i=0; i<dataSize; i++) {
      data[i] = input.readShort();
    }
    array = (ArrayShort) Array.factory(short.class, shape, data);
  }

  public ArrayShort getArray() {
    return array;
  }

  @Override
  public void setArray(Array array) {
    this.array = (ArrayShort) array;
  }

  public void setArray(ArrayShort array) {
    this.array = array;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    short[] storage = (short[]) array.getStorage();
    int[] shape = array.getShape();

    out.writeInt(shape.length);
    for (int i=0; i<shape.length; i++) {
      out.writeInt(shape[i]);
    }

    out.writeInt(storage.length);
    for (int i = 0; i<storage.length; i++) {
      out.writeShort(storage[i]);
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
    short[] data = new short[dataSize];
    for (int i=0; i<dataSize; i++) {
      data[i] = in.readShort();
    }

    array = (ArrayShort) Array.factory(short.class, shape, data);
  }
}
