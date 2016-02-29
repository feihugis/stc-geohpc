package edu.gmu.stc.hadoop.raster;

/**
 * Created by Fei Hu on 2/27/16.
 */
public class RasterUtils {

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


  public static void main(String[] args) {
    Float[] input = new Float[] {1.2f, 3.2f, 4.3f};
    System.out.println(RasterUtils.arrayToString(input));
  }

}
