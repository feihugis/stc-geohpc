package edu.gmu.stc.hadoop.raster.grib1;

import edu.gmu.stc.hadoop.raster.DataChunk;

/**
 * Created by Fei Hu on 4/19/16.
 */
public class Grib1Chunk extends DataChunk {

  public Grib1Chunk() {}


  public Grib1Chunk(String shortName, String path, int[] corner, int[] shape, String[] dimensions, long filePos, long byteSize,
                 int filterMask, String[] hosts, String dataType) {
    super(corner, shape, dimensions, filePos, byteSize, filterMask, hosts, dataType, shortName, path);
  }

}
