package edu.gmu.stc.website;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Fei Hu on 4/24/16.
 */
public class WebProperties {
  public static String GIF_PATH = "";
  public static String MERRA2_DAILY_INPUTPATH = "";
  public static String SPARK_HOME = "";
  public static String SPARK_JAR_PATH = "";
  public static String SPARK_MASTER = "";
  public static String SPARK_GEOEXTRA_MERRA2_CLASS = "";
  public static String SPARK_TAYLORDIAGRAM_CLASS = "";
  public static String CFSR_PATH = "";
  public static String CMAP_PATH = "";
  public static String GPCP_PATH = "";
  public static String TAYLORDIAGRAM_JSON_OUTPUT = "";
  public static String TAYLORDIAGRAM_PYTHON_SCRIPT = "";
  public static String TAYLORDIAGRAM_RESULT_PATH = "";
  public static String RESULT_URI = "";
  public static String RESULT_PATH = "";
  public static String MERRA2_TAYLORDIAGRAM_INPUT = "";


  public static void initilizeProperties() {
    Log log = LogFactory.getLog(WebProperties.class);
    String filename = "/website.properties";
    Properties properties = new Properties();
    try {
      InputStream inputStream = new WebProperties().getClass().getResourceAsStream(filename);    //resource.getInputStream();
      properties.load(inputStream);
      inputStream.close();
    } catch (Exception ex) {
      if (log != null) {
        log.warn("Unable to access file <" + filename + ">.");
      }
    }

    GIF_PATH = (String) getOption(properties, "gif_path", "/test");
    MERRA2_DAILY_INPUTPATH = (String) getOption(properties, "merra2_daily_inputpath", "/test");
    SPARK_HOME = (String) getOption(properties, "spark_home", "/test");
    SPARK_JAR_PATH = (String) getOption(properties, "spark_jar_path", "/test");
    SPARK_MASTER = (String) getOption(properties, "spark_master", "/test");
    SPARK_GEOEXTRA_MERRA2_CLASS = (String) getOption(properties, "spark_geoextra_merra2_class", "/test");
    SPARK_TAYLORDIAGRAM_CLASS = (String) getOption(properties, "spark_taylordiagram_class", "/test");
    CFSR_PATH = (String) getOption(properties, "cfsr_path", "/test");
    CMAP_PATH = (String) getOption(properties, "cmap_path", "/test");
    GPCP_PATH = (String) getOption(properties, "gpcp_path", "/test");
    TAYLORDIAGRAM_JSON_OUTPUT = (String) getOption(properties, "taylordiagram_json_output", "/test");
    TAYLORDIAGRAM_PYTHON_SCRIPT = (String) getOption(properties, "taylordiagram_python_script", "/test");
    TAYLORDIAGRAM_RESULT_PATH = (String) getOption(properties, "taylordiagram_result_path", "/test");
    RESULT_URI = (String) getOption(properties, "result_uri", "/test");
    RESULT_PATH = (String) getOption(properties, "result_path", "/test");
    MERRA2_TAYLORDIAGRAM_INPUT = (String) getOption(properties, "merra2_taylordiagram_input", "/test");
  }

 /* static {
    Log log = LogFactory.getLog(WebProperties.class);
    String filename = "/website.properties";
    Properties properties = new Properties();
    try {
      InputStream inputStream = new WebProperties().getClass().getResourceAsStream(filename);    //resource.getInputStream();
      properties.load(inputStream);
      inputStream.close();
    } catch (Exception ex) {
      if (log != null) {
        log.warn("Unable to access file <" + filename + ">.");
      }
    }

    GIF_PATH = (String) getOption(properties, "gif_path", "/test");
    MERRA2_DAILY_INPUTPATH = (String) getOption(properties, "merra2_daily_inputpath", "/test");
  }*/

  private static Object getOption(Properties properties, String currentProperty, String defaultVal) {
    String value = defaultVal;

    // attempt to load it from properties file
    if (properties.getProperty(currentProperty) != null) {
      // attempt to load it from config file
      value = properties.getProperty(currentProperty);
    }
    return value;
  }

}
