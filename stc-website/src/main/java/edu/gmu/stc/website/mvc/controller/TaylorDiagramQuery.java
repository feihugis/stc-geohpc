package edu.gmu.stc.website.mvc.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.util.StringHelper;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import edu.gmu.stc.datavisualization.taylordiagram.TaylorDiagramFactory;
import edu.gmu.stc.website.WebProperties;
import ucar.ma2.InvalidRangeException;

/**
 * Created by Fei Hu on 5/8/16.
 */

@RestController
public class TaylorDiagramQuery {
  private static final Log LOG = LogFactory.getLog(TaylorDiagramQuery.class);

  @RequestMapping(value = "/query/taylordiagram", method = RequestMethod.GET)
  public String queryMerra2test(@RequestParam Map<String, String> requestParams) throws ClassNotFoundException, IOException, InterruptedException, ParseException, InvalidRangeException {

    String start_time = requestParams.get("start_time"); //"198001";
    String end_time = requestParams.get("end_time");  //"198012";
    String min_lat = requestParams.get("min_lat");  //"-90.00";
    String max_lat = requestParams.get("max_lat");  //"90.00";
    String min_lon = requestParams.get("min_lon");  //"-180.00";
    String max_lon = requestParams.get("max_lon");  //"180.00";
    String isCMAP = requestParams.get("isCMAP");
    String isGPCP = requestParams.get("isGPCP");
    String isMERRA2 = requestParams.get("isMERRA2");
    String isMERRA1 = requestParams.get("isMERRA1");
    String isCFSR = requestParams.get("isCFSR");
    String isERAINTRIM = requestParams.get("isERAINTRIM");

    WebProperties.initilizeProperties();
    String spark_master = WebProperties.SPARK_MASTER;  //"local[6]";
    String merra2Path = WebProperties.MERRA2_TAYLORDIAGRAM_INPUT; //"/Users/feihu/Documents/Data/Merra2/monthly";
    String cfsr_path = WebProperties.CFSR_PATH;  //"/Users/feihu/Documents/Data/CFSR/pgbh06.gdas.A_PCP.SFC.grb2";
    String cmap_path = WebProperties.CMAP_PATH;  //"/Users/feihu/Documents/Data/CMAP/precip.mon.mean.standard.nc";
    String gpcp_path = WebProperties.GPCP_PATH; //"/Users/feihu/Documents/Data/GPCPV2.2/precip.mon.mean.nc";
    String json_output = WebProperties.TAYLORDIAGRAM_JSON_OUTPUT;  //"/Users/feihu/Desktop/taylordiagram.json";
    String tydPythonScript = WebProperties.TAYLORDIAGRAM_PYTHON_SCRIPT;  //"/Users/feihu/Documents/GitHub/stc-geohpc/stc-datavisualization/src/main/java/edu/gmu/stc/datavisualization/python/TaylorDiagram.py"; //"/Users/feihu/Desktop/taylordiagram-v2.py";

    String x_axis = "Precipitation";
    String y_axis = "values-mm/day";
    String taylordiagram_file_nanme = start_time + "-" + end_time + "-" + min_lat + "-" + max_lat + "-" + min_lon + "-" + max_lon
                                      + isCMAP + isGPCP + isMERRA2 + isMERRA1 + isCFSR + isERAINTRIM + ".png";
    String tyd_output = WebProperties.TAYLORDIAGRAM_RESULT_PATH + taylordiagram_file_nanme;  //"/Users/feihu/Desktop/taylordiagram-reanalysis.png";
    String result_uri = WebProperties.RESULT_URI+"taylordiagram/" + taylordiagram_file_nanme;
    String shellscript = "sh " + WebProperties.SPARK_HOME + " "
                         + "--class " + WebProperties.SPARK_TAYLORDIAGRAM_CLASS + " "
                         + WebProperties.SPARK_JAR_PATH + " "
                         + start_time + " "
                         + end_time + " "
                         + min_lat + " "
                         + max_lat + " "
                         + min_lon + " "
                         + max_lon + " "
                         + spark_master + " "
                         + merra2Path + " "
                         + cfsr_path + " "
                         + cmap_path + " "
                         + gpcp_path + " "
                         + json_output + " "
                         + tydPythonScript + " "
                         + x_axis + " "
                         + y_axis + " "
                         + tyd_output + " "
                         + isCMAP + " "
                         + isGPCP + " "
                         + isMERRA2 + " "
                         + isMERRA1 + " "
                         + isCFSR + " "
                         + isERAINTRIM;

    LOG.info("taylordiagram service shell : " + shellscript);
    Process ps = Runtime.getRuntime().exec(shellscript);
    int shellState = ps.waitFor();
    LOG.info("*** shell state" + shellState  );

    return  result_uri;
  }

}
