package edu.gmu.stc.hadoop.raster;

import org.apache.hadoop.fs.FileSystem;

import java.util.List;

import ucar.nc2.Variable;

/**
 * Created by Fei Hu on 8/24/16.
 */
public abstract class VarLayoutParser {

  public abstract List<DataChunk> layoutParse(Variable var, String filePath, FileSystem fs);

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
