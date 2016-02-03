package edu.gmu.stc.spark.test.query.merra.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.FloatType$;

import java.util.ArrayList;
import java.util.List;

import scala.collection.mutable.IndexedSeq;
import scala.collection.mutable.Seq;
import scala.collection.mutable.WrappedArray;

import scala.reflect.ClassTag;
import ucar.ma2.ArrayFloat;

/**
 * Created by Fei Hu on 1/29/16.
 */
public class SpaceQuery implements UDF3<WrappedArray<Float>, String, String, List<Float>> {
  private static final Log LOG = LogFactory.getLog(SpaceQuery.class);

  @Override
  public List<Float> call(WrappedArray<Float> input, String start, String end) throws Exception {
    int[] from = string2intArray(start, "-");
    int[] to = string2intArray(end, "-");
    int[] shape = new int[]{361, 540}; // need input the shape information

    return subset(shape, from, to, input);
  }

  public int[] string2intArray(String value, String separator) {
    String[] valueArray = value.split(separator);
    int[] results = new int[valueArray.length];
    for(int i = 0; i < results.length; i++) {
      results[i] = Integer.parseInt(valueArray[i]);
    }

    return results;
  }

  public List<Float> subset(int[] shape, int[] start, int[] end, WrappedArray<Float> input) {
    List<Float> result = new ArrayList<Float>();
    switch (shape.length) {
      case 2: {
        for (int i = start[0]; i < end[0]; i++) {
          for (int j = start[1]; j < end[1]; j++) {
            result.add(input.apply(j*shape[1] + shape[0]));
          }
        }
        break;
      }

      case 3: {
        for (int i = start[0]; i < end[0]; i++) {
          for (int j = start[1]; j < end[1]; j++) {
            for (int m = start[2]; m < end[2]; m++) {
              result.add(input.apply(shape[0]*shape[1]*m + j*shape[1] + shape[0]));
            }
          }
        }
        break;
      }
    }

    return result;
  }

}
