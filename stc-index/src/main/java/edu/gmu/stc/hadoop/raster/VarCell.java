package edu.gmu.stc.hadoop.raster;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Created by Fei Hu on 2/19/16.
 */
public abstract class VarCell implements Writable{
  int[] corner = null;         //relative to the whole picture
  int[] shape = null;          //chunk shape
  String[] dimensions = null;  //dimension info for each dimension
  Number fillValue = null;     // fillvalue for the missing-value points
  float[] scales = null;       // the physical units for the coordinates
  String dataType;             //the data type
  List<DataChunk> dataChunks = null;

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

    Text.writeString(out, dataType);
    if (dataType.equals("Float") || dataType.equals("float") ) {
      out.writeFloat((Float) fillValue);
    } else if (dataType.equals("Double") || dataType.equals("double")) {
      out.writeDouble((Double) fillValue);
    } else if (dataType.equals("Integer") || dataType.equals("int")) {
      out.writeInt((Integer) fillValue);
    } else if (dataType.equals("Long") || dataType.equals("long")) {
      out.writeLong((Long) fillValue);
    }
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

    dataType = Text.readString(in);
    if (dataType.equals("Float") || dataType.equals("float") ) {
      fillValue = in.readFloat();
    } else if (dataType.equals("Double") || dataType.equals("double")) {
      fillValue = in.readDouble();
    } else if (dataType.equals("Integer") || dataType.equals("int")) {
      fillValue = in.readInt();
    } else if (dataType.equals("Long") || dataType.equals("long")) {
      fillValue = in.readLong();
    }
  }

}
