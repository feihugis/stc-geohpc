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

import edu.gmu.stc.hadoop.index.io.merra.NcHdfsRaf;
import edu.gmu.stc.hadoop.raster.hdf5.ArrayFloatSerializer;
import ucar.ma2.ArrayFloat;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

/**
 * Created by Fei Hu on 4/19/16.
 */
public class DataChunkReader extends RecordReader<DataChunk, ArrayFloatSerializer> {
  private static final Log LOG = LogFactory.getLog(DataChunkReader.class);
  private DataChunkInputSplit dataChunkInputSplit;
  private List<DataChunk> dataChunkList = new ArrayList<DataChunk>();
  private int currentKeyMark = -1;
  private int keySize = 0;
  private boolean debug= false;
  private Configuration conf;
  private FSDataInputStream inputStream;
  private FileSystem fs;
  private NetcdfFile nc;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    dataChunkInputSplit = (DataChunkInputSplit) inputSplit;
    dataChunkList = dataChunkInputSplit.getChunkList();
    keySize = dataChunkList.size();
    this.conf = taskAttemptContext.getConfiguration();
    this.fs = FileSystem.get(this.conf);
    DataChunk dataChunk = this.dataChunkList.get(0);
    Path path = new Path(dataChunk.getFilePath());
    NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(path), fs.getConf());
    nc = NetcdfFile.open(raf, path.toString());
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
  public ArrayFloatSerializer getCurrentValue() throws IOException, InterruptedException {
    DataChunk dataChunk = this.dataChunkList.get(currentKeyMark);
    ArrayFloatSerializer value = null;

    List<Variable> variables = nc.getVariables();
    for (Variable var: variables) {
      if (var.getShortName().equals(dataChunk.getVarShortName())) {
        ArrayFloat v = null;
        try {
           v = (ArrayFloat) var.read(dataChunk.corner, dataChunk.shape);
        } catch (InvalidRangeException e) {
          e.printStackTrace();
        }
        value = new ArrayFloatSerializer(v);
        break;
      }
    }

    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {

  }
}
