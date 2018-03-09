package edu.gmu.stc.hadoop.vector.geojson;

import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.hadoop.vector.Polygon;

/**
 * Created by Fei Hu on 2/21/16.
 */
public class PolygonJSON {
  String type;
  List<List<Double[]>> coordinates = new ArrayList<>();

  public Polygon toPolygon() {
    List<Double[]> pointList = coordinates.get(0);
    int npoints = pointList.size();
    double[] xpoints = new double[npoints];
    double[] ypoints = new double[npoints];
    for(int i=0; i <npoints; i++) {
      xpoints[i] = pointList.get(i)[0];
      ypoints[i] = pointList.get(i)[1];
    }

    return new Polygon(xpoints, ypoints, npoints);
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public List<List<Double[]>> getCoordinates() {
    return coordinates;
  }

  public void setCoordinates(List<List<Double[]>> coordinates) {
    this.coordinates = coordinates;
  }
}
