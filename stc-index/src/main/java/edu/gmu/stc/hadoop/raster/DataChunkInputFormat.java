package edu.gmu.stc.hadoop.raster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.configure.MyProperty;
import edu.gmu.stc.database.DBConnector;
import edu.gmu.stc.hadoop.commons.ClimateHadoopConfigParameter;
import edu.gmu.stc.hadoop.raster.index.DataChunkIndexBuilderImp;

/**
 * Created by Fei Hu on 8/26/16.
 */
public class DataChunkInputFormat extends FileInputFormat {

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    Configuration conf = job.getConfiguration();

    int start_time = conf.getInt(ClimateHadoopConfigParameter.QUERY_TIME_START, 0);
    int end_time = conf.getInt(ClimateHadoopConfigParameter.QUERY_TIME_END, 0);
    String[] varShortNames = conf.getStrings(ClimateHadoopConfigParameter.QUERY_VARIABLE_NAMES);
    String[] geometryInfos = conf.getStrings(ClimateHadoopConfigParameter.QUERY_GEOMETRY_INFO);
    String filePath_prefix = conf.get(ClimateHadoopConfigParameter.INDEX_FILEPATH_PREFIX);
    String filePath_suffix = conf.get(ClimateHadoopConfigParameter.INDEX_FILEPATH_SUFFIX);

    String dbHost = conf.get(ClimateHadoopConfigParameter.DB_HOST);
    String dbPort = conf.get(ClimateHadoopConfigParameter.DB_Port);
    String dbName = conf.get(ClimateHadoopConfigParameter.DB_DATABASE_NAME);
    String dbUser = conf.get(ClimateHadoopConfigParameter.DB_USERNAME);
    String dbPWD = conf.get(ClimateHadoopConfigParameter.DB_PWD);

    Statement statement = new DBConnector(dbHost, dbPort, dbName, dbUser, dbPWD).GetConnStatement();

    DataChunkIndexBuilderImp dataChunkIndexBuilderImp = new DataChunkIndexBuilderImp(filePath_prefix , filePath_suffix);
    List<DataChunk> dataChunkList = new ArrayList<DataChunk>();

    for (String var : varShortNames) {
      dataChunkList.addAll(dataChunkIndexBuilderImp.queryDataChunks(var, start_time, end_time, geometryInfos, statement));
    }

    List<InputSplit> dataChunkInputSplitList = new ArrayList<InputSplit>();
    dataChunkInputSplitList.addAll(ChunkUtils.generateInputSplitByHosts(dataChunkList));

    return dataChunkInputSplitList;
  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    //Can use DataChunkReaderByNetCDFLib to evaluate the DataChunkReader
    //return new DataChunkReaderByNetCDFLib();
    return new DataChunkReader();
  }
}
