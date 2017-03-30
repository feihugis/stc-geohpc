package edu.gmu.stc.hadoop.raster;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Fei Hu on 3/29/17.
 */
public class GenericRecordReader extends RecordReader<Text, Text> {
  private List<FileSplit> fileSplitList = new ArrayList<>();
  private int len = 0;
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    fileSplitList.add(null);   // this is for nextKeyValue to remove the first element

    FileSplit fileSplit = (FileSplit) split;
    fileSplitList.add(fileSplit);
    len = fileSplitList.size();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    fileSplitList.remove(0);
    return !fileSplitList.isEmpty();
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return new Text(fileSplitList.get(0).getPath().toString());
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return new Text(fileSplitList.get(0).getPath().toString());
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 1 - fileSplitList.size()*1.0F/len;
  }

  @Override
  public void close() throws IOException {

  }
}
