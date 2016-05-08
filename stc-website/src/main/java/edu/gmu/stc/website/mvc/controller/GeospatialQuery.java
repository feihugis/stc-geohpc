package edu.gmu.stc.website.mvc.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.gmu.stc.website.WebProperties;


/**
 * Created by Fei Hu on 4/23/16.
 */

@RestController
public class GeospatialQuery {

  @RequestMapping(value = "/query/merra2", method = RequestMethod.GET)
  public List<String> queryMerra2(@RequestParam Map<String, String> requestParams) throws ClassNotFoundException, IOException, InterruptedException {
    String inputPath = WebProperties.MERRA2_DAILY_INPUTPATH; //"/Users/feihu/Documents/Data/Merra2/";
    String vars = requestParams.get("vars");
    String startTime = requestParams.get("startTime");
    String endTime = requestParams.get("endTime");
    String statenames = requestParams.get("statename");
    String isObject = requestParams.get("isObject");
    String isGlobal = requestParams.get("isGlobal");
    String outputFilePath = WebProperties.GIF_PATH + statenames; //add statenames to path to differ different spatial query for variables

    List<String> results = getImgLinkPaths(outputFilePath, vars);
    if (results.size()>0) {
      return results;
    }

    /*String[] inputs = new String[9];
    inputs[0] = "local[6]";
    inputs[1] = vars;
    inputs[2] = inputPath;
    inputs[3] = startTime;
    inputs[4] = endTime;
    inputs[5] = statenames;
    inputs[6] = isObject;
    inputs[7] = isGlobal;
    inputs[8] = outputPath;
    GeoSpatialExtraction.main(inputs);*/
    String shellscript = "sh " + WebProperties.SPARK_HOME +" "
                         + "--class " + WebProperties.SPARK_GEOEXTRA_MERRA2_CLASS + " "
                         + WebProperties.SPARK_JAR_PATH+" "
                         + WebProperties.SPARK_MASTER + " "
                         + vars + " "
                         + inputPath + " "
                         + startTime + " "
                         + endTime + " "
                         + statenames + " "
                         + isObject + " "
                         + isGlobal + " "
                         + outputFilePath;   //"/Applications/apache-tomcat-8.0.14/webapps/gif/";

    // sh /Users/feihu/Documents/GitHub/Spark/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --class edu.gmu.stc.spark.io.etl.GeoSpatialExtraction /Users/feihu/Documents/GitHub/stc-geohpc/stc-spark/target/stc-spark-1.0-SNAPSHOT.jar local[6] EVAP /Users/feihu/Documents/Data/Merra2/ 19800101 20151201 Alaska,Hawaii,Puerto false true /Applications/apache-tomcat-8.0.14/webapps/gif/
    System.out.println(shellscript);
    Process ps = Runtime.getRuntime().exec(shellscript);
    int shellState = ps.waitFor();
    System.out.println("*** shell " + shellState );
    results = getImgLinkPaths(WebProperties.GIF_PATH, vars + statenames);
    return  results;
  }

  //TODO add temporal query   UFLXKE
  public List<String> getImgLinkPaths(String folderPath, String key) {
    File folder = new File(folderPath);
    File[] fileList = folder.listFiles();
    List<String> results = new ArrayList<String>();

    for (int i=0; i< fileList.length; i++) {
      String localPath = fileList[i].getName();
      if (localPath.contains(key)) {
        results.add("/gif/" + localPath);
      }
    }

    return results;
  }

  @RequestMapping(value = "/query/merra2test", method = RequestMethod.GET)
  public List<String> queryMerra2test(@RequestParam Map<String, String> requestParams) throws ClassNotFoundException, IOException, InterruptedException {
    List<String> results = new ArrayList<String>();
    results.add("1-test");
    results.add("2-test");
    return  results;
  }

}
