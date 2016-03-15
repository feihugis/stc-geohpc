/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.gmu.stc.hadoop.vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.postgis.Geometry;
import org.postgis.LinearRing;
import org.postgis.PGgeometry;


import java.awt.*;
import java.awt.geom.Rectangle2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


import edu.gmu.stc.hadoop.io.TextSerializerHelper;

/**
 * A class that represents a polygon using a list of points.
 * @author eldawy
 *
 */
public class Polygon implements Shape {
  private static final Log LOG = LogFactory.getLog(Polygon.class);
  private static final long serialVersionUID = -117491486038680078L;
  double[] xpoints;
  double[] ypoints;
  int npoints;

  public Polygon() {

  }

  public Polygon(double[] xpoints, double[] ypoints, int npoints) {
    this.npoints = npoints;
    this.xpoints = new double[npoints];
    this.ypoints = new double[npoints];
    System.arraycopy(xpoints, 0, this.xpoints, 0, npoints);
    System.arraycopy(ypoints, 0, this.ypoints, 0, npoints);
  }

  /**
   * Set the points in the rectangle to the given array
   * @param xpoints
   * @param ypoints
   * @param npoints
   */
  public void set(double[] xpoints, double[] ypoints, int npoints) {
    this.npoints = npoints;
    this.xpoints = new double[npoints];
    this.ypoints = new double[npoints];
    System.arraycopy(xpoints, 0, this.xpoints, 0, npoints);
    System.arraycopy(ypoints, 0, this.ypoints, 0, npoints);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(npoints);
    for (int i = 0; i < npoints; i++) {
      out.writeDouble(xpoints[i]);
      out.writeDouble(ypoints[i]);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.npoints = in.readInt();
    this.xpoints = new double[npoints];
    this.ypoints = new double[npoints];
    
    for (int i = 0; i < npoints; i++) {
      this.xpoints[i] = in.readDouble();
      this.ypoints[i] = in.readDouble();
    }
  }

  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeInt(npoints, text, ',');
    for (int i = 0; i < npoints; i++) {
      TextSerializerHelper.serializeDouble(xpoints[i], text, ',');
      TextSerializerHelper.serializeDouble(ypoints[i], text,
          i == npoints - 1 ? '\0' : ',');
    }
    return text;
  }

  @Override
  public void fromText(Text text) {
    this.npoints = TextSerializerHelper.consumeInt(text, ',');
    this.xpoints = new double[npoints];
    this.ypoints = new double[npoints];
    
    for (int i = 0; i < npoints; i++) {
      this.xpoints[i] = TextSerializerHelper.consumeDouble(text, ',');
      this.ypoints[i] = TextSerializerHelper.consumeDouble(text,
          i == npoints - 1 ? '\0' : ',');
    }
  }

  @Override
  public Rectangle getMBR() {
    double x_min = Double.MAX_VALUE, x_max = -1*Double.MAX_VALUE, y_min = Double.MAX_VALUE, y_max = -1*Double.MAX_VALUE;
    for (int i = 0; i < npoints; i++) {
      if (xpoints[i] > x_max) x_max = xpoints[i];
      if (xpoints[i] < x_min) x_min = xpoints[i];
      if (ypoints[i] > y_max) y_max = ypoints[i];
      if (ypoints[i] < y_min) y_min = ypoints[i];
    }
    return new Rectangle(x_min, y_min, x_max, y_max);
  }

  @Override
  public double distanceTo(double x, double y) {
    throw new RuntimeException("Not implemented yet");
  }

  @Override
  public boolean isIntersected(Shape s) {
    throw new RuntimeException("Not implemented yet");
  }
  
  public Polygon clone() {
    return new Polygon(xpoints, ypoints, npoints);
  }

  @Override
  public void draw(Graphics g, Rectangle fileMBR, int imageWidth,
                   int imageHeight, double scale) {
    throw new RuntimeException("Not implemented yet");
  }
  
  @Override
  public void draw(Graphics g, double xscale, double yscale) {
    throw new RuntimeException("Not implemented yet");
  }

  public double[] getXpoints() {
    return xpoints;
  }

  public void setXpoints(double[] xpoints) {
    this.xpoints = xpoints;
  }

  public double[] getYpoints() {
    return ypoints;
  }

  public void setYpoints(double[] ypoints) {
    this.ypoints = ypoints;
  }

  public int getNpoints() {
    return npoints;
  }

  public void setNpoints(int npoints) {
    this.npoints = npoints;
  }

  public Point getPoint(int index) {
    return new Point(xpoints[index], ypoints[index]);
  }

  public org.postgis.Polygon toPostGISPolygon() {
    org.postgis.Point[] points = new org.postgis.Point[npoints+1];
    for (int i=0; i<npoints; i++) {
      points[i] = new org.postgis.Point(xpoints[i], ypoints[i]);
    }
    points[npoints] = new org.postgis.Point(xpoints[0], ypoints[0]);
    org.postgis.LinearRing[] linearRing = new LinearRing[] {new LinearRing(points)};
    return new org.postgis.Polygon(linearRing);
  }

  public org.postgis.PGgeometry toPostGISPGgeometry() {
    return new PGgeometry(toPostGISPolygon());
  }

  public static Polygon generatePolygonFromPGgeometry(PGgeometry plgn) {
    if (plgn.getGeoType() == Geometry.POLYGON) {
      org.postgis.Polygon polygon = (org.postgis.Polygon) plgn.getGeometry();
      LinearRing ring = polygon.getRing(0);
      org.postgis.Point[] points = ring.getPoints();
      double[] xpoints = new double[points.length-1];
      double[] ypoints = new double[points.length-1];
      for (int i=0; i<points.length-1; i++) {
        xpoints[i] = points[i].getX();
        ypoints[i] = points[i].getY();
      }
      return new Polygon(xpoints, ypoints, points.length-1);
    } else {
      LOG.info("There is a bug in edu.gmu.stc.hadoop.vector.Polygon.generatePolygonFromPGgeometry" );
    }
    return null;
  }

  public Polygon toLogicView(double xResolution, double yResolution, double x_orig, double y_orig) {
    double[] xs = new double[npoints];
    double[] ys= new double[npoints];
    for (int i = 0; i < npoints; i++) {
      xs[i] = (xpoints[i] - x_orig) / xResolution;
      ys[i] = (ypoints[i] - y_orig) / yResolution;
    }

    return new Polygon(xs, ys, npoints);
  }

  public boolean contains(double x, double y) {
    if (npoints <= 2 || !getMBR().contains(x, y)) {
      return false;
    }
    int hits = 0;

    double lastx = xpoints[npoints - 1];
    double lasty = ypoints[npoints - 1];
    double curx, cury;

    // Walk the edges of the polygon
    for (int i = 0; i < npoints; lastx = curx, lasty = cury, i++) {
      curx = xpoints[i];
      cury = ypoints[i];

      if (cury == lasty) {
        continue;
      }

      double leftx;
      if (curx < lastx) {
        if (x >= lastx) {
          continue;
        }
        leftx = curx;
      } else {
        if (x >= curx) {
          continue;
        }
        leftx = lastx;
      }

      double test1, test2;
      if (cury < lasty) {
        if (y < cury || y >= lasty) {
          continue;
        }
        if (x < leftx) {
          hits++;
          continue;
        }
        test1 = x - curx;
        test2 = y - cury;
      } else {
        if (y < lasty || y >= cury) {
          continue;
        }
        if (x < leftx) {
          hits++;
          continue;
        }
        test1 = x - lastx;
        test2 = y - lasty;
      }

      if (test1 < (test2 / (lasty - cury) * (lastx - curx))) {
        hits++;
      }
    }

    return ((hits & 1) != 0);
  }

  public static void main(String[] args) {
    Polygon plgn = new Polygon(new double[]{0.0, 1.0, 1.0, 0.0}, new double[]{0.0, 0.0,1.0,1.0}, 4);
    System.out.println(plgn.toPostGISPGgeometry().toString());
  }
}
