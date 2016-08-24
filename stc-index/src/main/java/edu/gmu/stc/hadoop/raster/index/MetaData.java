package edu.gmu.stc.hadoop.raster.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.gmu.stc.hadoop.vector.Polygon;
import ucar.nc2.iosp.hdf5.H5header;

/**
 * Created by Fei Hu on 2/25/16.
 */
public class MetaData {

  public static class MERRA2 {
    public static int latChunkShape = 91;
    public static int lonChunkShape = 144;
    public static String latDim = "lat";
    public static String lonDim = "lon";
    public static double latUnit = 0.5;
    public static double lonUnit = 0.625;
    public static int latShape = 361;
    public static int lonShape = 576;
    public static float fillValue = 9.9999999E14f;
    public static float vmax = 9.9999999E14f;
    public static float vmin = -9.9999999E14f;
    public static float lon_orig = -180.0f;
    public static float lat_orig = -90.0f;

    public static HashMap<int[],Polygon> getChunkBoundaries() {
      HashMap<int[], Polygon> chunkBoundaries = new HashMap<int[], Polygon>();
      int numLatChunks = (latShape%latChunkShape>0)? latShape/latChunkShape+1:latShape/latChunkShape;
      int numLonChunks = (lonShape%lonChunkShape>0)? lonShape/lonChunkShape+1:lonShape/lonChunkShape;
      for (int i = 0; i < numLatChunks; i++) {
        for (int j = 0; j < numLonChunks; j++) {
          double[] lats = new double[4];
          double[] lons = new double[4];
          lats[0] = -90.0 + latUnit*latChunkShape*i;
          lats[1] = -90.0 + latUnit*latChunkShape*i;
          lats[2] = -90.0 + latUnit*(latChunkShape*(i+1)-1);
          lats[3] = -90.0 + latUnit*(latChunkShape*(i+1)-1);

          lons[0] = -180.0 + lonUnit*lonChunkShape*j;
          lons[1] = -180.0 + lonUnit*(lonChunkShape*(j+1)-1);
          lons[2] = -180.0 + lonUnit*(lonChunkShape*(j+1)-1);
          lons[3] = -180.0 + lonUnit*lonChunkShape*j;
          int[] corner = new int[] {latChunkShape*i, lonChunkShape*j};
          chunkBoundaries.put(corner, new Polygon(lons, lats, 4));
        }
      }

      //for lon variable
      chunkBoundaries.put(new int[]{0}, new Polygon(new double[]{-180.0, -180.0, 179.375, 179.375},
                                                    new double[]{0.0, 0.0, 0.0, 0.0},
                                                    4));
      //for lat variable
      chunkBoundaries.put(new int[]{0}, new Polygon(new double[]{0.0, 0.0, 0.0, 0.0},
                                                    new double[]{-90.0, 90.0, 90.0, -90.0},
                                                    4));

      //for time variable
      chunkBoundaries.put(new int[]{0}, new Polygon(new double[]{0.0, 0.0, 0.0, 0.0},
                                                    new double[]{0.0, 0.0, 0.0, 0.0},
                                                    4));
      return chunkBoundaries;
    }

    public static List<Integer> xyTogeometryID(double x_min, double y_min, double x_max, double y_max) {
      int x_corner = (int) ((x_min - lon_orig)/lonUnit);
      int y_corner = (int) ((y_min - lat_orig)/latUnit);
      int x_shape = (int) ((x_max - x_min)/lonUnit);
      int y_shape = (int) ((y_max - y_min)/latUnit);

      int start_geometryID = y_corner/MetaData.MERRA2.latChunkShape*4 + x_corner/MetaData.MERRA2.lonChunkShape;
      int end_geometryID = (y_corner + y_shape)/MetaData.MERRA2.latChunkShape * 4 + (x_corner + x_shape)/MetaData.MERRA2.lonChunkShape;

      int lon_size = (end_geometryID - start_geometryID)%4;
      int lat_size = (end_geometryID - start_geometryID)/4;
      ArrayList<Integer> geometryIDs = new ArrayList<Integer>();
      for (int y=0; y<=lat_size; y++) {
        for (int x=0; x<=lon_size; x++) {
          int geometryID = start_geometryID + y*4 + x;
          geometryIDs.add(geometryID);
        }
      }

      return geometryIDs;
    }
  }

}
