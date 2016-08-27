package edu.gmu.stc.hadoop.raster;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.hadoop.raster.hdf5.H5Chunk;

/**
 * Created by Fei Hu on 3/8/16.
 */
public class DataChunkInputSplit extends InputSplit implements Writable {
  protected List<DataChunk> chunkList = new ArrayList<DataChunk>();  //Each data chunk should be located on the same hosts

  public DataChunkInputSplit() {

  }

  public DataChunkInputSplit(List<DataChunk> chunkList) {
    this.chunkList = chunkList;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return chunkList.size();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return chunkList.get(0).getHosts();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(chunkList.size());
    for (DataChunk chunk : chunkList) {
      chunk.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    chunkList = new ArrayList<DataChunk>();
    int n = in.readInt();
    while (n>0) {
      DataChunk chunk = new DataChunk();
      chunk.readFields(in);
      chunkList.add(chunk);
      n = n -1;
    }
  }

  public List<DataChunk> getChunkList() {
    return chunkList;
  }

  public void setChunkList(List<DataChunk> chunkList) {
    this.chunkList = chunkList;
  }
}
