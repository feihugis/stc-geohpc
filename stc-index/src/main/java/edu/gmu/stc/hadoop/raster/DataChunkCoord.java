package edu.gmu.stc.hadoop.raster;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by Fei Hu on 1/1/17.
 */
public class DataChunkCoord implements Serializable, Writable, KryoSerializable {
  int[] corner;
  String[] hosts;

  public DataChunkCoord(DataChunk dataChunk) {
    this.corner = new int[dataChunk.getCorner().length + 1];
    this.corner[0] = dataChunk.getTime();
    for (int i = 1; i < corner.length; i++) {
      this.corner[i] = dataChunk.getCorner()[i - 1];
    }

    this.hosts = dataChunk.getHosts();
  }

  public DataChunkCoord(DataChunkCorner corner, DataChunkHosts hosts) {
    this.corner = corner.getCorner();
    this.hosts = hosts.getHosts();
  }

  public boolean physicalEqual(DataChunkCoord dataChunkCoord) {
    if (this.corner.length != dataChunkCoord.corner.length ||
        this.hosts.length != dataChunkCoord.hosts.length) {
      return false;
    }

    for (int i = 0; i < this.corner.length; i++) {
      if (this.corner[i] != dataChunkCoord.corner[i]) return false;
    }

    for (int i = 0; i < this.hosts.length; i++) {
      if (!this.hosts[i].equals(dataChunkCoord.hosts[i])) return false;
    }

    return true;
  }

  public boolean logicalEqual(DataChunkCoord dataChunkCoord) {
    if (this.corner.length != dataChunkCoord.corner.length) {
      return false;
    }

    for (int i = 0; i < this.corner.length; i++) {
      if (this.corner[i] != dataChunkCoord.corner[i]) return false;
    }

    return true;
  }


  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(corner.length);
    for (int i : corner) {
      out.writeInt(i);
    }

    out.writeInt(hosts.length);

    for (String h : hosts) {
      Text.writeString(out, h);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    this.corner = new int[size];
    for (int i = 0; i < size; i++) {
      this.corner[i] = in.readInt();
    }

    size = in.readInt();
    this.hosts = new String[size];
    for (int i = 0; i < size; i++) {
      this.hosts[i] = Text.readString(in);
    }
  }

  @Override
  public void write(Kryo kryo, Output output) {
    output.writeInt(corner.length);
    for (int i : corner) {
      output.writeInt(i);
    }

    output.writeInt(hosts.length);

    for (String h : hosts) {
      output.writeString(h);
    }
  }

  @Override
  public void read(Kryo kryo, Input input) {
    int size = input.readInt();
    this.corner = new int[size];
    for (int i = 0; i < size; i++) {
      this.corner[i] = input.readInt();
    }

    size = input.readInt();
    this.hosts = new String[size];
    for (int i = 0; i < size; i++) {
      this.hosts[i] = input.readString();
    }
  }

  public int[] getCorner() {
    return corner;
  }

  public double[] getCornerAsDouble() {
    double[] result = new double[this.corner.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = this.corner[i] + 0.0d;
    }
    return result;
  }

  public void setCorner(int[] corner) {
    this.corner = corner;
  }

  public String[] getHosts() {
    return hosts;
  }

  public void setHosts(String[] hosts) {
    this.hosts = hosts;
  }

  public int hashCode() {
    return Arrays.toString(this.corner).hashCode();
  }

  public double getTime() {
    return this.corner[0];
  }

  public double getID() {
    String id = "";
    for (int c : this.corner) {
      id += String.format("%03d", c);
    }
    return Double.parseDouble(id);
  }
}
