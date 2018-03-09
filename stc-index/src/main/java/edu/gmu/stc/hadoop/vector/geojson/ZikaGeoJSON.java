package edu.gmu.stc.hadoop.vector.geojson;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Fei Hu on 4/3/17.
 */
public class ZikaGeoJSON {
  String type;
  String crs;
  List<ZikaPolygonJSON> features = new ArrayList<>();

  public ZikaGeoJSON(String type, String crs,
                     List<ZikaPolygonJSON> features) {
    this.type = type;
    this.crs = crs;
    this.features = features;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getCrs() {
    return crs;
  }

  public void setCrs(String crs) {
    this.crs = crs;
  }

  public List<ZikaPolygonJSON> getFeatures() {
    return features;
  }

  public void setFeatures(List<ZikaPolygonJSON> features) {
    this.features = features;
  }
}
