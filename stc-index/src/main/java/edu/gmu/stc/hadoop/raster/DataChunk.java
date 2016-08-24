package edu.gmu.stc.hadoop.raster;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Fei Hu on 2/17/16.
 */
public class DataChunk implements Writable {
  int[] corner = null;         //relative to the whole picture
  int[] shape = null;          //chunk shape; to get the endcorner: corner[0] + shape[0] - 1
  String[] dimensions = null;  //dimension info for each dimension, such [time, lat, lon]
  long filePos = 0;            //the start location in the file
  long byteSize = 0;           // byte size for this chunk
  int filterMask = -1;         //compression type for HDF4; filter type for HDF5
  String[] hosts = null;       //the data nodes who host these data
  String dataType;             //the data type
  String varShortName;
  String filePath;
  boolean isContain = true;
  int time = 0;
  String geometryInfo = "";

  public DataChunk() {}

  public DataChunk(int[] corner, int[] shape, String[] dimensions, long filePos, long byteSize,
                   int filterMask, String[] hosts, String dataType, String varShortName, String filePath) {
    this.corner = corner;
    this.shape = shape;
    this.dimensions = dimensions;
    this.filePos = filePos;
    this.byteSize = byteSize;
    this.filterMask = filterMask;
    this.hosts = hosts;
    this.dataType = dataType;
    this.varShortName = varShortName;
    this.filePath = filePath;
  }

  public DataChunk(int[] corner, int[] shape, String[] dimensions, long filePos, long byteSize,
                   int filterMask, String[] hosts, String dataType, String varShortName, String filePath, int time) {
    this.corner = corner;
    this.shape = shape;
    this.dimensions = dimensions;
    this.filePos = filePos;
    this.byteSize = byteSize;
    this.filterMask = filterMask;
    this.hosts = hosts;
    this.dataType = dataType;
    this.varShortName = varShortName;
    this.filePath = filePath;
    this.time = time;
  }



  @Override
  public String toString() {
    String output = this.getVarShortName() + " corner : ";
    for (int corner : getCorner()) {
      output = output + corner + " ";
    }
    output += "start : " + getFilePos() + " end : " + (getFilePos() + getByteSize());
    return output;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(corner.length);
    for (int i=0; i<corner.length; i++) {
      out.writeInt(corner[i]);
    }

    for (int i=0; i<shape.length; i++) {
      out.writeInt(shape[i]);
    }

    for (int i=0; i<dimensions.length; i++) {
      Text.writeString(out, dimensions[i]);
    }

    out.writeLong(filePos);
    out.writeLong(byteSize);
    out.writeInt(filterMask);

    out.writeInt(hosts.length);
    for (int i=0; i<hosts.length; i++) {
      Text.writeString(out, hosts[i]);
    }

    Text.writeString(out, dataType);
    Text.writeString(out, varShortName);
    Text.writeString(out, filePath);

    out.writeBoolean(isContain);
    out.writeInt(this.time);

    Text.writeString(out, geometryInfo);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int num = in.readInt();
    corner = new int[num];
    shape = new int[num];
    dimensions = new String[num];

    for (int i=0; i<num; i++) {
      corner[i] = in.readInt();
    }

    for (int i=0; i<num; i++) {
      shape[i] = in.readInt();
    }

    for (int i=0; i<num; i++) {
      dimensions[i] = Text.readString(in);
    }

    filePos = in.readLong();
    byteSize = in.readLong();
    filterMask = in.readInt();

    num = in.readInt();
    hosts = new String[num];
    for (int i =0; i<num; i++) {
      hosts[i] = Text.readString(in);
    }

    dataType = Text.readString(in);
    varShortName = Text.readString(in);
    filePath = Text.readString(in);
    isContain = in.readBoolean();
    this.time = in.readInt();
    this.geometryInfo = Text.readString(in);
  }


/*  @Override
  public void write(Kryo kryo, Output out) {
    kryo.writeObjectOrNull(out, this, this.getClass());
    *//*out.writeInt(corner.length);
    for (int i=0; i<corner.length; i++) {
      out.writeInt(corner[i]);
    }

    for (int i=0; i<shape.length; i++) {
      out.writeInt(shape[i]);
    }

    for (int i=0; i<dimensions.length; i++) {
      out.writeString(dimensions[i]);
    }

    out.writeLong(filePos);
    out.writeLong(byteSize);
    out.writeInt(filterMask);

    out.writeInt(hosts.length);
    for (int i=0; i<hosts.length; i++) {
      out.writeString(hosts[i]);
    }

    out.writeString(dataType);
    out.writeString(varShortName);
    out.writeString(filePath);

    out.writeBoolean(isContain);
*//*
  }*/
/*
  @Override
  public void read(Kryo kryo, Input in) {
     kryo.readObjectOrNull(in, this.getClass());
    *//*int num = in.readInt();
    corner = new int[num];
    shape = new int[num];
    dimensions = new String[num];

    for (int i=0; i<num; i++) {
      corner[i] = in.readInt();
    }

    for (int i=0; i<num; i++) {
      shape[i] = in.readInt();
    }

    for (int i=0; i<num; i++) {
      dimensions[i] = in.readString();
    }

    filePos = in.readLong();
    byteSize = in.readLong();
    filterMask = in.readInt();

    num = in.readInt();
    hosts = new String[num];
    for (int i =0; i<num; i++) {
      hosts[i] = in.readString();
    }

    dataType = in.readString();
    varShortName = in.readString();
    filePath = in.readString();
    isContain = in.readBoolean();*//*
  }*/

  public boolean isContain() {
    return isContain;
  }

  public void setContain(boolean contain) {
    isContain = contain;
  }

  public int[] getCorner() {
    return corner;
  }

  public void setCorner(int[] corner) {
    this.corner = corner;
  }

  public int[] getShape() {
    return shape;
  }

  public void setShape(int[] shape) {
    this.shape = shape;
  }

  public String[] getDimensions() {
    return dimensions;
  }

  public void setDimensions(String[] dimensions) {
    this.dimensions = dimensions;
  }

  public long getFilePos() {
    return filePos;
  }

  public void setFilePos(long filePos) {
    this.filePos = filePos;
  }

  public long getByteSize() {
    return byteSize;
  }

  public void setByteSize(long byteSize) {
    this.byteSize = byteSize;
  }

  public int getFilterMask() {
    return filterMask;
  }

  public void setFilterMask(int filterMask) {
    this.filterMask = filterMask;
  }

  public String[] getHosts() {
    return hosts;
  }

  public void setHosts(String[] hosts) {
    this.hosts = hosts;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public String getVarShortName() {
    return varShortName;
  }

  public void setVarShortName(String varShortName) {
    this.varShortName = varShortName;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public int getTime() {return  time; }

  public void setTime(int time) { this.time = time;}

  public String getGeometryInfo() {
    return geometryInfo;
  }

  public void setGeometryInfo(String geometryInfo) {
    this.geometryInfo = geometryInfo;
  }
}
