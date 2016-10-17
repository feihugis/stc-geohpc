package edu.gmu.stc.spark.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.configure.MyProperty;
import edu.gmu.stc.database.DBConnector;
import edu.gmu.stc.hadoop.commons.ClimateDataMetaParameter;
import edu.gmu.stc.hadoop.commons.ClimateHadoopConfigParameter;
import edu.gmu.stc.hadoop.raster.ChunkUtils;
import edu.gmu.stc.hadoop.raster.DataChunk;
import edu.gmu.stc.hadoop.raster.index.DataChunkIndexBuilder;
import edu.gmu.stc.hadoop.raster.index.DataChunkIndexBuilderImp;
import edu.gmu.stc.spark.io.kryo.SparkKryoRegistrator;

/**
 * Created by Fei Hu on 8/25/16.
 */
public class STIndexOperator {

  public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException {
    //args = new String[]{""};
    if (args.length != 1) {
      System.err.println("Please input <configFilePath>");
      System.exit(2);
    }

    Configuration configuration = new Configuration();
    //configuration.addResource("merra100-climatespark-config.xml");
    configuration.addResource(new Path(args[0]));

    String inputDir = configuration.get(ClimateHadoopConfigParameter.MR_FILEINPUTFORMAT_INPUT_DIR);
    System.out.println("*****" + inputDir);

    final String productName = configuration.get(ClimateDataMetaParameter.CLIMATE_DATA_PRODUCT);
    final String filePath_prefix = configuration.get(ClimateHadoopConfigParameter.INDEX_FILEPATH_PREFIX);
    final String filePath_suffix = configuration.get(ClimateHadoopConfigParameter.INDEX_FILEPATH_SUFFIX);
    String inputFile_Filter = configuration.get(ClimateHadoopConfigParameter.INDEX_INPUTFILE_FILTER);
    String index_variables = configuration.get(ClimateHadoopConfigParameter.INDEX_VARIABLES);

    String fileFormat = configuration.get(ClimateDataMetaParameter.CLIMATE_DATA_FORMAT);
    String groupName = configuration.get(ClimateDataMetaParameter.CLIMATE_DATA_PRODUCT_GROUP);

    final String dbHost = configuration.get(ClimateHadoopConfigParameter.DB_HOST);
    final String dbPort = configuration.get(ClimateHadoopConfigParameter.DB_Port);
    final String dbName = configuration.get(ClimateHadoopConfigParameter.DB_DATABASE_NAME);
    final String dbUser = configuration.get(ClimateHadoopConfigParameter.DB_USERNAME);
    final String dbPWD = configuration.get(ClimateHadoopConfigParameter.DB_PWD);

    String spark_Master = configuration.get(ClimateHadoopConfigParameter.SPARK_MASTER);

    final List<String> inputFiles = STIndexUtils.getInputPaths(configuration, inputDir, fileFormat, inputFile_Filter);

    //get the specified variables. If it is null, then get all the variables
    //Aerosol_Optical_Depth_Land_Ocean_Mean
    final List<String> allVarNames = ChunkUtils.getSpecifiedVariables(inputFiles.get(0), groupName, index_variables);

    String variables = StringUtils.join(",", allVarNames);

    System.out.println(variables);


    DataChunkIndexBuilderImp dataChunkIndexBuilderImp = new DataChunkIndexBuilderImp(filePath_prefix, filePath_suffix);

    Statement statement = new DBConnector(dbHost, dbPort, dbUser, dbPWD).GetConnStatement();
    dataChunkIndexBuilderImp.createDatabase(dbName, dbUser, statement);
    statement.getConnection().close();

    statement = new DBConnector(dbHost, dbPort, dbName, dbUser, dbPWD).GetConnStatement();

    for (String varName : allVarNames) {
      dataChunkIndexBuilderImp.createTable(varName, statement);
    }

    statement.getConnection().close();

    final SparkConf sconf = new SparkConf().setAppName("SparkIndexBuilder"); //.setMaster("local[6]");
    //sconf.setMaster(spark_Master);

    sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sconf.set("spark.kryo.registrator", SparkKryoRegistrator.class.getName());

    JavaSparkContext sc = new JavaSparkContext(sconf);

    JavaRDD<String> varNameRDD = sc.parallelize(allVarNames);

    varNameRDD.foreach(new VoidFunction<String>() {
      @Override
      public void call(String varName) throws Exception {
        DataChunkIndexBuilderImp dataChunkIndexBuilderImp = new DataChunkIndexBuilderImp(filePath_prefix, filePath_suffix);
        Statement statement = new DBConnector(dbHost, dbPort, dbName, dbUser, dbPWD).GetConnStatement();
        List<DataChunk> chunkList = new ArrayList<DataChunk>();

        for (String filePath : inputFiles) {
          chunkList.addAll(ChunkUtils.geneDataChunksByVar(filePath, varName));
        }

        dataChunkIndexBuilderImp.insertDataChunks(chunkList, varName, statement);
        statement.getConnection().close();
      }
    });

  }
}
