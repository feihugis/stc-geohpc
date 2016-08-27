package edu.gmu.stc.spark.index;

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
    args = new String[] {"/config.txt.example", "/Users/feihu/Documents/Data/modis_hdf/MOD08_D3", "mod08",
                         "/Users/feihu/Documents/Data/modis_hdf/MOD08_D3", ".hdf", ".hdf"};
    String groupName = "Data_Fields";  //null
    //args = new String[] {"/asdf/", "/Users/feihu/Documents/Data/M2T1NXINT/1980/01", "M2T1NXINT",
    //                     "/Users/feihu/Documents/Data/M2T1NXINT/1980/01", ".nc4", ".nc4"};
    if (args.length != 6) {
      System.err.println("Please input <configFilePath> <inputDir> <productName> <filePath_prefix> <filePath_suffix> <fileFormat>");
      System.exit(2);
    }

    String cfgFile = args[0];
    String inputDir = args[1];
    final String productName = args[2];
    final String filePath_prefix = args[3];
    final String filePath_suffix = args[4];
    String fileFormat = args[5];

    DataChunkIndexBuilderImp dataChunkIndexBuilderImp = new DataChunkIndexBuilderImp(filePath_prefix, filePath_suffix);

    final List<String> inputFiles = STIndexUtils.getInputPaths(inputDir, fileFormat);
    List<String> allVarNames = ChunkUtils.getAllVarShortNames(inputFiles.get(0), groupName);

    //Statement statement = new DBConnector().GetConnStatement();
    Statement statement = new DBConnector(MyProperty.db_host + productName, MyProperty.mysql_user, MyProperty.mysql_password, productName).GetConnStatement();

    for (String varName : allVarNames) {
      dataChunkIndexBuilderImp.createTable(varName, statement);
    }

    final SparkConf sconf = new SparkConf().setAppName("SparkIndexBuilder").setMaster("local[6]");
    sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sconf.set("spark.kryo.registrator", SparkKryoRegistrator.class.getName());

    JavaSparkContext sc = new JavaSparkContext(sconf);

    JavaRDD<String> inputFileRDD = sc.parallelize(allVarNames);

    inputFileRDD.foreach(new VoidFunction<String>() {
      @Override
      public void call(String varName) throws Exception {
        DataChunkIndexBuilderImp dataChunkIndexBuilderImp = new DataChunkIndexBuilderImp(filePath_prefix, filePath_suffix);
        Statement statement = new DBConnector(MyProperty.db_host + productName, MyProperty.mysql_user, MyProperty.mysql_password, productName).GetConnStatement();
        List<DataChunk> chunkList = new ArrayList<DataChunk>();
        for (String filePath : inputFiles) {
          chunkList.addAll(ChunkUtils.geneDataChunksByVar(filePath, varName));
        }
        dataChunkIndexBuilderImp.insertDataChunks(chunkList, varName, statement);
      }
    });








  }
}
