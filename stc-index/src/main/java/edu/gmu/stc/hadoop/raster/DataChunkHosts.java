package edu.gmu.stc.hadoop.raster;

import java.io.Serializable;

/**
 * Created by Fei Hu on 1/9/17.
 */
public class DataChunkHosts implements Serializable{
  private String[] hosts;

  public DataChunkHosts(String[] hosts) {
    this.hosts = hosts;
  }

  public DataChunkHosts(DataChunk dataChunk) {
    this.hosts = dataChunk.getHosts();
  }

  public String[] getHosts() {
    return hosts;
  }

  public void setHosts(String[] hosts) {
    this.hosts = hosts;
  }


}
