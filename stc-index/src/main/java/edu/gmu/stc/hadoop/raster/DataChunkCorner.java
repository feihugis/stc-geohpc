package edu.gmu.stc.hadoop.raster;

import java.io.Serializable;

/**
 * Created by Fei Hu on 1/9/17.
 *
 * this corner is different from the corner in datachunk class. It combine the time and the chunk corner information
 */
public class DataChunkCorner implements Serializable {
  private int[] corner;

  public DataChunkCorner(int[] corner) {
    this.corner = corner;
  }

  public DataChunkCorner(Integer[] corner) {
    this.corner = RasterUtils.IntegerToint(corner);
  }

  public DataChunkCorner(DataChunk dataChunk) {
    this.corner = new int[dataChunk.getCorner().length + 1];
    this.corner[0] = dataChunk.getTime();
    for (int i = 1; i < corner.length; i++) {
      this.corner[i] = dataChunk.getCorner()[i-1];
    }
  }

  public int[] getCorner() {
    return corner;
  }

  public void setCorner(int[] corner) {
    this.corner = corner;
  }

  public double[] toDouble() {
    double[] result = new double[this.corner.length];
    for (int i = 0; i < this.corner.length; i++) {
      result[i] = corner[i] + 0.0d;
    }
    return result;
  }

  public double getID() {
    String id = "";
    for (int c : this.corner) {
      id += String.format("%03d", c);
    }
    return Double.parseDouble(id);
  }
}
