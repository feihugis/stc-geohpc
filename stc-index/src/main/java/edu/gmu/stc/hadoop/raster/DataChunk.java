package edu.gmu.stc.hadoop.raster;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Fei Hu on 2/17/16.
 */
public abstract class DataChunk implements Writable{
  int[] corner = null;         //relative to the whole picture
  int[] shape = null;          //chunk shape
  String[] dimensions = null;  //dimension info for each dimension
  long filePos = 0;            //the start location in the file
  long byteSize = 0;           // byte size for this chunk
  int filterMask = -1;         //compression type for HDF4; filter type for HDF5
  String[] hosts = null;       //the data nodes who host these data
  String dataType;             //the data type

  public DataChunk(int[] corner, int[] shape, String[] dimensions, long filePos, long byteSize,
                   int filterMask, String[] hosts, String dataType) {
    this.corner = corner;
    this.shape = shape;
    this.dimensions = dimensions;
    this.filePos = filePos;
    this.byteSize = byteSize;
    this.filterMask = filterMask;
    this.hosts = hosts;
    this.dataType = dataType;
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
}
