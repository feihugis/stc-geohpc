package edu.gmu.stc.hadoop.raster.hdf5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.hadoop.raster.DataChunk;
import edu.gmu.stc.hadoop.raster.DataChunkInputSplit;

/**
 * Created by Fei Hu on 3/8/16.
 */
public class H5ChunkInputSplit extends DataChunkInputSplit {

  public H5ChunkInputSplit(List<DataChunk> chunkList) {
    super(chunkList);
  }

  public H5ChunkInputSplit(){

  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.chunkList.size());
    for (DataChunk chunk : chunkList) {
      chunk.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    chunkList = new ArrayList<DataChunk>();
    int n = in.readInt();
    while (n>0) {
      H5Chunk chunk = new H5Chunk();
      chunk.readFields(in);
      chunkList.add(chunk);
      n = n -1;
    }
  }
}
