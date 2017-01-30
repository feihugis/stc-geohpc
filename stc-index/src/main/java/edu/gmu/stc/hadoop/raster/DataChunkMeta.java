package edu.gmu.stc.hadoop.raster;

import java.io.Serializable;

/**
 * Created by Fei Hu on 1/10/17.
 */
public class DataChunkMeta implements Serializable{

  private int[] shape = null;
  private String[] dimensions = null;
  private int filterMask = -1;
  private String dataType;
  private String varShortName;
  private String filePathPattern;
  private String nodeIndexPattern;

  public DataChunkMeta(int[] shape, String[] dimensions, int filterMask, String dataType,
                       String varShortName, String filePathPattern, String nodeIndexPattern) {
    this.shape = shape;
    this.dimensions = dimensions;
    this.filterMask = filterMask;
    this.dataType = dataType;
    this.varShortName = varShortName;
    this.filePathPattern = filePathPattern;
    this.nodeIndexPattern = nodeIndexPattern;
  }

  //TODO: to make it more general
  public String getFilePath(int time) {
    String filePath;
    if (time <= 19911231) {
      // /merra2/daily/M2T1NXINT/YEAR/MONTH/MERRA2_100.tavg1_2d_int_Nx.TIME.nc4
      filePath = this.filePathPattern.replace("CODE", "100");
    } else {
      // /merra2/daily/M2T1NXINT/1992/12/MERRA2_200.tavg1_2d_int_Nx.19921201.nc4
      filePath = this.filePathPattern.replace("CODE", "200");
    }

    filePath = filePath.replace("YEAR", time/10000 + "");
    String month = String.format("%02d", (time % 10000) / 100);
    filePath = filePath.replace("MONTH", month);
    filePath = filePath.replace("TIME", time + "");
    return filePath;
  }

  public int[] getShape() {
    return shape;
  }

  public void setShape(int[] shape) {
    this.shape = shape;
  }

  public String[] getDimensions() {
    return dimensions;
  }

  public void setDimensions(String[] dimensions) {
    this.dimensions = dimensions;
  }

  public int getFilterMask() {
    return filterMask;
  }

  public void setFilterMask(int filterMask) {
    this.filterMask = filterMask;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public String getVarShortName() {
    return varShortName;
  }

  public void setVarShortName(String varShortName) {
    this.varShortName = varShortName;
  }

  public String getFilePathPattern() {
    return filePathPattern;
  }

  public void setFilePathPattern(String filePathPattern) {
    this.filePathPattern = filePathPattern;
  }

  public String getNodeIndexPattern() {
    return nodeIndexPattern;
  }

  public void setNodeIndexPattern(String nodeIndexPattern) {
    this.nodeIndexPattern = nodeIndexPattern;
  }
}
