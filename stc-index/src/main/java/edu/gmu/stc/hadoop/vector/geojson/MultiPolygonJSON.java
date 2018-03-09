package edu.gmu.stc.hadoop.vector.geojson;

import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.hadoop.vector.Polygon;

/**
 * Created by Fei Hu on 2/21/16.
 */
public class MultiPolygonJSON {
  String type;
  List<List<List<Double[]>>> coordinates = new ArrayList<>();

  public List<Polygon> toMultiPolygons() {
    List<Polygon> plgnList = new ArrayList<Polygon>();
    for (List<List<Double[]>> plgn : coordinates) {
      List<Double[]> pointList = plgn.get(0);
      int npoints = pointList.size();
      double[] xpoints = new double[npoints];
      double[] ypoints = new double[npoints];
      for(int i=0; i <npoints; i++) {
        xpoints[i] = pointList.get(i)[0];
        ypoints[i] = pointList.get(i)[1];
      }
      plgnList.add(new Polygon(xpoints, ypoints, npoints));
    }

    return plgnList;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public List<List<List<Double[]>>> getCoordinates() {
    return coordinates;
  }

  public void setCoordinates(List<List<List<Double[]>>> coordinates) {
    this.coordinates = coordinates;
  }
}
