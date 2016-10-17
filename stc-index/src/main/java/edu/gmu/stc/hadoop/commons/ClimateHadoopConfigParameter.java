package edu.gmu.stc.hadoop.commons;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by Fei Hu on 8/27/16.
 */
public class ClimateHadoopConfigParameter {
  //************Database Configuration
  public static final String DB_HOST = "db.host";
  public static final String DB_Port = "db.port";
  public static final String DB_DATABASE_NAME = "db.databasename";
  public static final String DB_USERNAME = "db.username";
  public static final String DB_PWD = "db.pwd";


  //************ Hadoop Configuration
  public static final String FS_DEFAULTFS = "fs.defaultFS";
  public static final String FS_HDFS_IMPL = "fs.hdfs.impl";
  public static final String FS_FILE_IMPL = "fs.file.impl";
  public static final String MR_FILEINPUTFORMAT_INPUT_DIR_RECURSIVE = "mapreduce.input.fileinputformat.input.dir.recursive";

  //************ Spark Configuration
  public static final String SPARK_MASTER = "spark.master";


  //************ Index Configuration
  public static final String MR_FILEINPUTFORMAT_INPUT_DIR = "mapreduce.input.fileinputformat.inputdir";
  public static final String INDEX_FILEPATH_PREFIX = "index.filepath.prefix";
  public static final String INDEX_FILEPATH_SUFFIX = "index.filepath.suffix";
  public static final String INDEX_INPUTFILE_FILTER= "index.inputfile.filter";
  public static final String INDEX_VARIABLES = "index.vairalbes";

  //************ Spatiotemporal Query
  public static final String QUERY_VARIABLE_NAMES = "query.variable.names";
  public static final String QUERY_TIME_START = "query.time.start";
  public static final String QUERY_TIME_END = "query.time.end";
  public static final String QUERY_GEOMETRY_INFO = "query.geometry.info";
  public static final String QUERY_SPACE_CORNER = "query.space.corner";
  public static final String QUERY_SPACE_SHAPE = "query.space.shape";

  public static void main(String[] args) {
    Configuration configuration = new Configuration();
    configuration.addResource("hadoop-cfg-test.xml");
    System.out.println(configuration.getInts(ClimateHadoopConfigParameter.QUERY_SPACE_CORNER).length);
  }
}
