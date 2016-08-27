package edu.gmu.stc.hadoop.raster;

import org.apache.hadoop.conf.Configuration;
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
import edu.gmu.stc.hadoop.raster.index.DataChunkIndexBuilderImp;

/**
 * Created by Fei Hu on 8/26/16.
 */
public class DataChunkInputFormat extends FileInputFormat {

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    Configuration conf = job.getConfiguration();
    int start_time = conf.getInt("start_time", 0);
    int end_time = conf.getInt("end_time", 0);
    String[] varShortNames = conf.getStrings("var_shortNames");
    String[] geometryInfos = conf.getStrings("geometryinfo");
    String filePath_prefix = conf.get("filePath_prefix");
    String filePath_suffix = conf.get("filePath_suffix");

    Statement statement = new DBConnector(MyProperty.db_host + MyProperty.mysql_catalog,
                                          MyProperty.mysql_user, MyProperty.mysql_password,
                                          MyProperty.mysql_catalog).GetConnStatement();

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
