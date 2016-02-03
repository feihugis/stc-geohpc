package edu.gmu.stc.spark.test.query.merra.io;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Fei Hu on 2/1/16.
 */
public class MerraVar implements Serializable {
  String shortName;
  int time;
  List<Integer> shape;
  String filePath;
  List<Float> values;

  public MerraVar() {
  }

  public MerraVar(String shortName, int time, List<Integer> shape,
                  List<Float> values) {
    this.shortName = shortName;
    this.time = time;
    this.shape = shape;
    this.values = values;

  }

  public String getShortName() {
    return shortName;
  }

  public void setShortName(String shortName) {
    this.shortName = shortName;
  }

  public int getTime() {
    return time;
  }

  public void setTime(int time) {
    this.time = time;
  }

  public List<Integer> getShape() {
    return shape;
  }

  public void setShape(List<Integer> shape) {
    this.shape = shape;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public List<Float> getValues() {
    return values;
  }

  public void setValues(List<Float> values) {
    this.values = values;
  }
}
