package edu.gmu.stc.hadoop.vector.extension;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.hadoop.vector.Polygon;

/**
 * Created by Fei Hu on 2/21/16.
 */
public class CountyFeature implements Feature, Writable{
  String type;
  String GEO_ID;
  String STATE;
  String COUNTY;
  String NAME;
  String LSAD;
  String CENSUSAREA;
  List<Polygon> polygonList = null;

  public CountyFeature() {

  }

  public CountyFeature(String type, String GEO_ID, String STATE, String COUNTY, String NAME,
                       String LSAD, String CENSUSAREA,
                       List<Polygon> polygonList) {
    this.type = type;
    this.GEO_ID = GEO_ID;
    this.STATE = STATE;
    this.COUNTY = COUNTY;
    this.NAME = NAME;
    this.LSAD = LSAD;
    this.CENSUSAREA = CENSUSAREA;
    this.polygonList = polygonList;
  }

  public CountyFeature(String type, String GEO_ID, String STATE, String COUNTY, String NAME,
                       String LSAD, String CENSUSAREA,
                       Polygon polygon) {
    this.type = type;
    this.GEO_ID = GEO_ID;
    this.STATE = STATE;
    this.COUNTY = COUNTY;
    this.NAME = NAME;
    this.LSAD = LSAD;
    this.CENSUSAREA = CENSUSAREA;
    this.polygonList = new ArrayList<Polygon>();
    polygonList.add(polygon);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, type);
    Text.writeString(out, GEO_ID);
    Text.writeString(out, STATE);
    Text.writeString(out, COUNTY);
    Text.writeString(out, NAME);
    Text.writeString(out, LSAD);
    Text.writeString(out, CENSUSAREA);

    out.writeInt(polygonList.size());
    for (Polygon plgn : polygonList) {
      plgn.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    type = Text.readString(in);
    GEO_ID = Text.readString(in);
    STATE = Text.readString(in);
    COUNTY = Text.readString(in);
    NAME = Text.readString(in);
    LSAD = Text.readString(in);
    CENSUSAREA = Text.readString(in);
    polygonList = new ArrayList<Polygon>();

    int number = in.readInt();
    for (int i=0; i<number; i++) {
      Polygon plgn = new Polygon();
      plgn.readFields(in);
      polygonList.add(plgn);
    }
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

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

  public List<Polygon> getPolygonList() {
    return polygonList;
  }

  public void setPolygonList(List<Polygon> polygonList) {
    this.polygonList = polygonList;
  }

  @Override
  public List<Polygon> getFeature() {
    return this.polygonList;
  }
}
