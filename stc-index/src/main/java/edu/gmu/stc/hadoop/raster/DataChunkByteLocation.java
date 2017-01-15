package edu.gmu.stc.hadoop.raster;

import java.io.Serializable;

/**
 * Created by Fei Hu on 1/9/17.
 */
public class DataChunkByteLocation implements Serializable {
  private long filePos = 0;            //the start location in the file
  private long byteSize = 0;           // byte size for this chunk

  public DataChunkByteLocation(long filePos, long byteSize) {
    this.filePos = filePos;
    this.byteSize = byteSize;
  }

  public DataChunkByteLocation(DataChunk dataChunk) {
    this.filePos = dataChunk.getFilePos();
    this.byteSize = dataChunk.getByteSize();
  }

  public String toString() {
    return filePos + "," + byteSize;
  }

  public long getFilePos() {
    return filePos;
  }

  public void setFilePos(long filePos) {
    this.filePos = filePos;
  }

  public long getByteSize() {
    return byteSize;
  }

  public void setByteSize(long byteSize) {
    this.byteSize = byteSize;
  }
}
