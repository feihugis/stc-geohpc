package edu.gmu.stc.hadoop.raster;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import scala.Serializable;


/**
 * Created by Fei Hu on 2/17/16.
 */

public class DataChunk implements Writable {
  //@Id @GeneratedValue(strategy = GenerationType.AUTO)
  private String id;
  private int[] corner = null;         //relative to the whole picture
  private int[] shape = null;          //chunk shape; to get the endcorner: corner[0] + shape[0] - 1
  private String[] dimensions = null;  //dimension info for each dimension, such [time, lat, lon]
  private long filePos = 0;            //the start location in the file
  private long byteSize = 0;           // byte size for this chunk
  private int filterMask = -1;         //compression type for HDF4; filter type for HDF5
  private String[] hosts = null;       //the data nodes who host these data
  private String dataType;             //the data type
  private String varShortName;
  private String filePath;
  private boolean isContain = true;
  private int time = 0;
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
    Arrays.sort(hosts);
    this.dataType = dataType;
    this.varShortName = varShortName;
    this.filePath = filePath;
    //this.id = varShortName + Arrays.toString(hosts) + time + corner[0] + geometryInfo;
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
    Arrays.sort(hosts);
    this.dataType = dataType;
    this.varShortName = varShortName;
    this.filePath = filePath;
    this.time = time;
    //this.id = varShortName + Arrays.toString(hosts) + time + corner[0] + geometryInfo;
  }

  public DataChunk(int[] corner, int[] shape, String[] dimensions, long filePos, long byteSize,
                   int filterMask, String[] hosts, String dataType, String varShortName, String filePath, int time, String geometryInfo) {
    this.corner = corner;
    this.shape = shape;
    this.dimensions = dimensions;
    this.filePos = filePos;
    this.byteSize = byteSize;
    this.filterMask = filterMask;
    this.hosts = hosts;
    Arrays.sort(hosts);
    this.dataType = dataType;
    this.varShortName = varShortName;
    this.filePath = filePath;
    this.time = time;
    this.geometryInfo = geometryInfo;
    //this.id = varShortName + Arrays.toString(hosts) + time + corner[0] + geometryInfo;
  }

  public DataChunk(Integer[] corner, Integer[] shape, String[] dimensions, Long filePos, Long byteSize,
                   Integer filterMask, String[] hosts, String dataType, String varShortName, String filePath, Integer time, String geometryInfo) {
    this.corner = RasterUtils.IntegerToint(corner);
    this.shape = RasterUtils.IntegerToint(shape);
    this.dimensions = dimensions;
    this.filePos = filePos;
    this.byteSize = byteSize;
    this.filterMask = filterMask;
    this.hosts = hosts;
    Arrays.sort(hosts);
    this.dataType = dataType;
    this.varShortName = varShortName;
    this.filePath = filePath;
    this.time = time;
    this.geometryInfo = geometryInfo;
    //this.id = varShortName + Arrays.toString(hosts) + time + corner[0] + geometryInfo;
  }

  public DataChunk(String metaData) {
    String[] meta = metaData.split(",");
    this.varShortName = meta[1];
    this.corner = RasterUtils.stringToIntArray(meta[2] + "," + meta[3] + "," + meta[4]);
    this.shape = RasterUtils.stringToIntArray(meta[5] + "," + meta[6] + "," + meta[7]);
    this.dimensions = RasterUtils.stringToStringArray(meta[8] + "," + meta[9] + "," + meta[10]);
    this.time = Integer.parseInt(meta[11]);
    this.dataType = meta[12];
    this.filePos = Long.parseLong(meta[13]);
    this.byteSize = Long.parseLong(meta[14]);
    this.filterMask = Integer.parseInt(meta[15]);
    this.hosts = RasterUtils.stringToStringArray(meta[16] + "," + meta[17] + "," + meta[18]);
    Arrays.sort(hosts);
    this.filePath = meta[19];
    this.geometryInfo = meta[20];

    //this.id = varShortName + Arrays.toString(hosts) + time + corner[0] + geometryInfo;
  }


  public String queryDataLocations() {
    return Arrays.toString(this.hosts);
  }

  @Override
  public String toString() {
    String output = this.getVarShortName() + " time : " + this.time
                    + " location: " + Arrays.toString(this.hosts)
                    + " corner : " + Arrays.toString(corner)
                    + "start : " + getFilePos() + " end : " + (getFilePos() + getByteSize());
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

  public int getShapeSize() {
    int size = 1;
    for (int i : shape) {
      size = size * i;
    }
    return size;
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

  public String getId() {
    return varShortName + Arrays.toString(hosts) + time + corner[0] + geometryInfo;
  }

  public long getCoordId() {
    String id = this.time + "";
    for (int c : corner) {
      id += c;
    }
    return Long.parseLong(id);
  }

  public void setId(String id) {
    this.id = id;
  }
}
