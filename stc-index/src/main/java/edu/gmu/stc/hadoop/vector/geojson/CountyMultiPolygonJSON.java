package edu.gmu.stc.hadoop.vector.geojson;

/**
 * Created by Fei Hu on 2/21/16.
 */
public class CountyMultiPolygonJSON {
  String type;
  PropertyJSON properties;
  MultiPolygonJSON geometry;

  public class PropertyJSON {
    String GEO_ID;
    String STATE;
    String COUNTY;
    String NAME;
    String LSAD;
    String CENSUSAREA;

    public String getGEO_ID() {
      return GEO_ID;
    }

    public void setGEO_ID(String GEO_ID) {
      this.GEO_ID = GEO_ID;
    }

    public String getSTATE() {
      return STATE;
    }

    public void setSTATE(String STATE) {
      this.STATE = STATE;
    }

    public String getCOUNTY() {
      return COUNTY;
    }

    public void setCOUNTY(String COUNTY) {
      this.COUNTY = COUNTY;
    }

    public String getNAME() {
      return NAME;
    }

    public void setNAME(String NAME) {
      this.NAME = NAME;
    }

    public String getLSAD() {
      return LSAD;
    }

    public void setLSAD(String LSAD) {
      this.LSAD = LSAD;
    }

    public String getCENSUSAREA() {
      return CENSUSAREA;
    }

    public void setCENSUSAREA(String CENSUSAREA) {
      this.CENSUSAREA = CENSUSAREA;
    }
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

  public void setProperties(
      PropertyJSON properties) {
      this.properties = properties;
  }

  public MultiPolygonJSON getGeometry() {
    return geometry;
  }

  public void setGeometry(MultiPolygonJSON geometry) {
    this.geometry = geometry;
  }
}
