package edu.gmu.stc.hadoop.raster;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Created by Fei Hu on 9/7/16.
 */
public class Cell implements KryoSerializable {
  private String varName;
  private int time;
  private int lat;
  private int lon;
  private float value;
  private String attributes;

  public Cell(String varName, int time, int lat, int lon, float value,
              String attributes) {
    this.varName = varName;
    this.time = time;
    this.lat = lat;
    this.lon = lon;
    this.value = value;
    this.attributes = attributes;
  }

  @Override
  public void write(Kryo kryo, Output output) {
    output.writeString(this.varName);
    output.writeInt(this.time);
    output.writeInt(this.lat);
    output.writeInt(this.lon);
    output.writeFloat(this.value);
    output.writeString(this.attributes);
  }

  @Override
  public void read(Kryo kryo, Input input) {
    this.varName = input.readString();
    this.time = input.readInt();
    this.lat = input.readInt();
    this.lon = input.readInt();
    this.value = input.readFloat();
    this.attributes = input.readString();
  }

  public String toString() {
    String s = java.lang.String.format("varName: %1$s; time: %2$d; lat+lon: (%3$d, %4$d); value: %5$f; attributes: %6$s",
                                       this.varName, this.time, this.lat, this.lon, this.value, this.attributes);
    return s;
  }

  public String getVarName() {
    return varName;
  }

  public void setVarName(String varName) {
    this.varName = varName;
  }

  public int getTime() {
    return time;
  }

  public void setTime(int time) {
    this.time = time;
  }

  public int getLat() {
    return lat;
  }

  public void setLat(int lat) {
    this.lat = lat;
  }

  public int getLon() {
    return lon;
  }

  public void setLon(int lon) {
    this.lon = lon;
  }

  public float getValue() {
    return value;
  }

  public void setValue(float value) {
    this.value = value;
  }

  public String getAttributes() {
    return attributes;
  }

  public void setAttributes(String attributes) {
    this.attributes = attributes;
  }
}
