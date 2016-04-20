package edu.gmu.stc.datavisualization.taylordiagram;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.List;

/**
 * Created by Fei Hu on 4/18/16.
 */
public class TaylorDiagramUnit {
  String legendName;
  float[] values = null;
  float std;
  float correlation;

  public TaylorDiagramUnit(String legendName, float[] values, float std, float correlation) {
    this.legendName = legendName;
    this.values = values;
    this.std = std;
    this.correlation = correlation;
  }

  public TaylorDiagramUnit(String legendName, Float[] values) {
    this.legendName = legendName;
    this.values = new float[values.length];
    for (int i=0; i<values.length; i++) {
      this.values[i] = values[i];
    }
  }

  public void calSTDAndPersonsCorrelation(double[] yArray) {
    DescriptiveStatistics stats = new DescriptiveStatistics();
    double[] xArray = new double[this.values.length];
    for (int i=0; i<values.length; i++) {
      xArray[i] = values[i];
      stats.addValue(values[i]);
    }
    this.std = (float) stats.getStandardDeviation();
    this.correlation = (float) new PearsonsCorrelation().correlation(xArray, yArray);
  }

  public String getLegendName() {
    return legendName;
  }

  public void setLegendName(String legendName) {
    this.legendName = legendName;
  }

  public float[] getValues() {
    return values;
  }

  public void setValues(float[] values) {
    this.values = values;
  }

  public float getStd() {
    return std;
  }

  public void setStd(float std) {
    this.std = std;
  }

  public float getCorrelation() {
    return correlation;
  }

  public void setCorrelation(float correlation) {
    this.correlation = correlation;
  }
}
