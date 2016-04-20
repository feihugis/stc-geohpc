package edu.gmu.stc.spark.io.etl;

import java.util.Date;

import javax.xml.crypto.Data;

/**
 * Created by Fei Hu on 3/30/16.
 */
public class TestEntrance {

  public static void main(String[] args) throws ClassNotFoundException {
    long date = System.currentTimeMillis();
    String[] meanArgs = new String[]{"LWTNET,UFLXKE", "/Users/feihu/Documents/Data/Merra2/", "[0-20,0-361,0-576],[21-23,0-361,0-576]", "19800101", "20151201",
                                     "/Users/feihu/Desktop/gz_2010_us_040_00_500k.json", "/Users/feihu/Desktop/test/"+date, "Alaska,Hawaii,Puerto", "false"};
    //ETLMeanLocalTest.main(meanArgs);
    ETLSDLocalTest.main(meanArgs);
  }
}
