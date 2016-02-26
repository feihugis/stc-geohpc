package edu.gmu.stc.hadoop.raster.index;


/**
 * Created by Fei Hu on 2/25/16.
 */
public class MetaData {

  public static class MERRA2 {
    final static int[] spaceChunkShape = new int[]{91, 144};
    final static String[] spaceDimensions = new String[]{"lat", "lon"};
    final static double[] spacenits = new double[]{0.5, 0.625};
    final static int[] spacechunkSize = new int[]{4, 4};
  }

}
