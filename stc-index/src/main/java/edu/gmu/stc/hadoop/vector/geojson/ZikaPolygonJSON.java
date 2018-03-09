package edu.gmu.stc.hadoop.vector.geojson;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Fei Hu on 4/2/17.
 */
public class ZikaPolygonJSON {
  String type;
  PropertyJSON properties;
  double[] bbox;
  MultiPolygonJSON geometry;

  public ZikaPolygonJSON(String type,
                         PropertyJSON properties, double[] bbox,
                         MultiPolygonJSON geometry) {
    this.type = type;
    this.properties = properties;
    this.bbox = bbox;
    this.geometry = geometry;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public PropertyJSON getProperties() {
    return properties;
  }

  public void setProperties(PropertyJSON properties) {
    this.properties = properties;
  }

  public double[] getBbox() {
    return bbox;
  }

  public void setBbox(double[] bbox) {
    this.bbox = bbox;
  }

  public MultiPolygonJSON getGeometry() {
    return geometry;
  }

  public void setGeometry(MultiPolygonJSON geometry) {
    this.geometry = geometry;
  }

  public class PropertyJSON {
    String STUSPS;
    String NAME;
    String ALAND;
    String AWATER;

    public PropertyJSON(String STUSPS, String NAME, String ALAND, String AWATER) {
      this.STUSPS = STUSPS;
      this.NAME = NAME;
      this.ALAND = ALAND;
      this.AWATER = AWATER;
    }

    public String getSTUSPS() {
      return STUSPS;
    }

    public void setSTUSPS(String STUSPS) {
      this.STUSPS = STUSPS;
    }

    public String getNAME() {
      return NAME;
    }

    public void setNAME(String NAME) {
      this.NAME = NAME;
    }

    public String getALAND() {
      return ALAND;
    }

    public void setALAND(String ALAND) {
      this.ALAND = ALAND;
    }

    public String getAWATER() {
      return AWATER;
    }

    public void setAWATER(String AWATER) {
      this.AWATER = AWATER;
    }
  }
}
