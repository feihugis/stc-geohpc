package edu.gmu.stc.hadoop.raster.hdf5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.gmu.stc.hadoop.raster.index.MetaData;
import edu.gmu.stc.hadoop.vector.Polygon;

/**
 * Created by Fei Hu on 2/28/16.
 */
public class Merra2Chunk extends H5Chunk{
  private Polygon boundary;  //to present the 2D boundary of this chunk

  public Merra2Chunk() {}

  public Merra2Chunk(String shortName, String path, int[] corner, int[] shape, String[] dimensions, long filePos, long byteSize,
                 int filterMask, String[] hosts, String dataType) {
    super( shortName, path, corner, shape, dimensions, filePos, byteSize, filterMask, hosts, dataType);
    initBoundary(corner, shape, dimensions);
  }

  public void initBoundary(int[] corner, int[] shape, String[] dimensions) {
    double[] lats = new double[4];
    double[] lons = new double[4];
    int latMark = -1, lonMark = -1;
    for (int i = 0; i<this.getDimensions().length; i++) {
      if (this.getDimensions()[i].equals("lat")) latMark = i;
      if (this.getDimensions()[i].equals("lon")) lonMark = i;
    }

    if (lonMark == -1) {
      lons[0] = 0.0;
      lons[1] = 0.0;
      lons[2] = 0.0;
      lons[3] = 0.0;
    } else {
      lons[0] = -180.0 + corner[lonMark] * MetaData.MERRA2.lonUnit;
      lons[1] = lons[0];
      lons[2] = -180.0 + (corner[lonMark] + shape[lonMark] - 1 )* MetaData.MERRA2.lonUnit;
      lons[3] = lons[2];
    }

    if (latMark == -1) {
      lats[0] = 0.0;
      lats[1] = 0.0;
      lats[2] = 0.0;
      lats[3] = 0.0;
    } else {
      lats[0] = -90.0 + corner[latMark] * MetaData.MERRA2.latUnit;
      lats[1] = -90.0 + (corner[latMark] + shape[latMark] - 1 )* MetaData.MERRA2.latUnit;
      lats[2] = lats[1];
      lats[3] = lats[0];
    }


    boundary = new Polygon(lons,lats, 4);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    boundary.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    boundary.readFields(in);
  }

  public Polygon getBoundary() {
    return boundary;
  }

  public void setBoundary(Polygon boundary) {
    this.boundary = boundary;
  }
}
