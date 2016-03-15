package edu.gmu.stc.hadoop.vector;

import org.apache.spark.sql.catalyst.expressions.If;

/**
 * Created by Fei Hu on 3/11/16.
 */
public class VectorOptor {

  public static boolean isPointInPolygon(Polygon plgn, Point point) {
    int[] xpoints = new int[plgn.getNpoints()];
    int[] ypoints = new int[plgn.getNpoints()];
    for (int i =0 ; i<xpoints.length; i++) {
      xpoints[i] = (int) plgn.getXpoints()[i];
      ypoints[i] = (int) plgn.getYpoints()[i];
    }

    java.awt.Polygon polygon = new java.awt.Polygon(xpoints, ypoints, plgn.getNpoints());

    return polygon.contains(point.x, point.y);

  }

  public static void main(String[] args) {
    Point point = new Point(0.5,1.0);
    Polygon plygon = new Polygon(new double[]{0.0,1.0,2.0,1.0,-1.0}, new double[]{0.0,0.0,1.0,2.0,1.0}, 5);

    if(VectorOptor.isPointInPolygon(plygon, point)) {
      System.out.println("Inside");
    }

  }

}
