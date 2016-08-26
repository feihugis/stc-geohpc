package edu.gmu.stc.hadoop.raster;

import org.apache.hadoop.fs.FileSystem;

import java.util.List;

import ucar.nc2.Variable;

/**
 * Created by Fei Hu on 8/24/16.
 */
public abstract class VarLayoutParser {

  public abstract List<DataChunk> layoutParse(Variable var, String filePath, FileSystem fs);

  public String getGeometryInfo(int[] varShape, int[] chunkCorner, int[]chunkShape){

    //here we assume that there is no chunk info for the one dimension data
    if (varShape.length<2) {
      return "0";
    }
    int y_index = varShape.length -2;
    int x_index = varShape.length -1;
    int y_DimShape = varShape[y_index];
    int x_DimShape = varShape[x_index];
    int y_ChunkShape = chunkShape[y_index];
    int x_ChunkShape = chunkShape[x_index];
    int y_corner = chunkCorner[y_index];
    int x_corner = chunkCorner[x_index];

    int geometryID = x_corner/x_ChunkShape + y_corner/y_ChunkShape*x_DimShape/x_ChunkShape;
    return geometryID + "";
  }

  /**
   * get the subString between 'start' and 'end' in the 'input'
   * @param input the string to be subsetted
   * @param start the start chars
   * @param end   the end chars
   * @return
   */
  public static String subString(String input, String start, String end) {
    return input.substring(input.indexOf(start) + start.length(), input.indexOf(end));
  }
}
