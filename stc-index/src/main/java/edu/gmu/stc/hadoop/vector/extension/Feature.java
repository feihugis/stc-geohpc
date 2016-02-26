package edu.gmu.stc.hadoop.vector.extension;

import java.util.List;

import edu.gmu.stc.hadoop.vector.Polygon;

/**
 * Created by Fei Hu on 2/25/16.
 */
public interface Feature {
  public List<Polygon> getFeature();
}
