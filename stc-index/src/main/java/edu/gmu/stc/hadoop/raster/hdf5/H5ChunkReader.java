package edu.gmu.stc.hadoop.raster.hdf5;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import edu.gmu.stc.hadoop.raster.DataChunk;

/**
 * Created by Fei Hu on 3/8/16.
 */
public class H5ChunkReader extends RecordReader<DataChunk, V> {

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return false;
  }

  @Override
  public DataChunk getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {

  }
}
