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

  public DataChunkMeta(int[] shape, String[] dimensions, int filterMask, String dataType,
                       String varShortName, String filePathPattern) {
    this.shape = shape;
    this.dimensions = dimensions;
    this.filterMask = filterMask;
    this.dataType = dataType;
    this.varShortName = varShortName;
    this.filePathPattern = filePathPattern;
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
}
