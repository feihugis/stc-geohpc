package edu.gmu.stc.hadoop.raster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.hadoop.raster.io.DataChunkIOProvider;
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer;
import ucar.ma2.Array;

/**
 * Created by Fei Hu on 8/26/16.
 */
public class DataChunkReader extends RecordReader<DataChunk, ArraySerializer> {
  private static final Log LOG = LogFactory.getLog(DataChunkReader.class);
  private List<DataChunk> dataChunkList = new ArrayList<DataChunk>();
  private int currentKeyMark = -1;
  private int keySize = 0;
  private boolean debug= false;
  private FSDataInputStream inputStream;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws
                                                                                       IOException, InterruptedException {
    DataChunkInputSplit dataChunkInputSplit = (DataChunkInputSplit) inputSplit;
    dataChunkList = dataChunkInputSplit.getChunkList();
    keySize = dataChunkList.size();
    Configuration conf = taskAttemptContext.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    DataChunk dataChunk = this.dataChunkList.get(0);
    Path path = new Path(dataChunk.getFilePath());
    inputStream = fs.open(path);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    currentKeyMark++;
    if (currentKeyMark < keySize) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public DataChunk getCurrentKey() throws IOException, InterruptedException {
    return this.dataChunkList.get(currentKeyMark);
  }

  @Override
  public ArraySerializer getCurrentValue() throws IOException, InterruptedException {
    DataChunk dataChunk = this.dataChunkList.get(currentKeyMark);
    ArraySerializer value = null;
    Array array = DataChunkIOProvider.read(dataChunk, inputStream);
    value = ArraySerializer.factory(array);
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return currentKeyMark*1.0f/keySize;
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }
}
