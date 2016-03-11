package edu.gmu.stc.hadoop.raster;

import org.apache.hadoop.fs.FileStatus;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Fei Hu on 2/27/16.
 */
public class RasterUtils {

  public static List<String> fileNamesToTableNames (List<String> files) {
    List<String> tableNames = new ArrayList<String>();
    for (int i=0; i<files.size(); i++) {
      String[] tmps = files.get(i).split("/");
      String tableName = tmps[tmps.length-1].toLowerCase();
      while (tableName.contains(".")) {
        tableName = tableName.replace(".", "_");
      }
      tableNames.add(tableName);
    }
    return tableNames;
  }

  public static List<String> fileStatusToTableNames (List<FileStatus> files) {
    List<String> tableNames = new ArrayList<String>();
    for (int i=0; i<files.size(); i++) {
      String[] tmps = files.get(i).getPath().toString().split("/");
      String tableName = tmps[tmps.length-1].toLowerCase();
      while (tableName.contains(".")) {
        tableName = tableName.replace(".", "_");
      }
      tableNames.add(tableName);
    }
    return tableNames;
  }



  public static String arrayToString(Object[] input) {
    String result = "[";
    for (int i=0; i<input.length; i++) {
      result = result + input[i] + ",";
    }
    result = result.substring(0, result.length()-1) + "]";

    return result;
  }

  public static Integer[] intToInteger(int[] input) {
    Integer[] result = new Integer[input.length];
    for (int i=0; i<input.length; i++) {
      result[i] = input[i];
    }

    return result;
  }


  public static String arrayToString(double[] input) {
    String result = "[";
    for (int i=0; i<input.length; i++) {
      result = result + input[i] + ",";
    }
    result = result.substring(0, result.length()-1) + "]";

    return result;
  }

  public static int[] IntegerToint(Integer[] input) {
    int[] output = new int[input.length];
    for (int i=0; i<input.length; i++) {
      output[i] = input[i];
    }
    return output;
  }

  public static boolean isShareHosts(DataChunk chunk1, DataChunk chunk2) {
    if (chunk2 == null) {
      return false;
    }
    String[] hosts1 = chunk1.getHosts();
    String[] hosts2 = chunk2.getHosts();

    for (int i=0; i<hosts1.length; i++) {
      for (int j = 0; j < hosts2.length; j++) {
        if (hosts1[i].equals(hosts2[j])) {
          return true;
        }
      }
    }
    return false;
  }


  public static void main(String[] args) {
    Float[] input = new Float[] {1.2f, 3.2f, 4.3f};
    System.out.println(RasterUtils.arrayToString(input));
  }

}
