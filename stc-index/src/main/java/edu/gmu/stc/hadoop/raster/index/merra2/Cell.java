package edu.gmu.stc.hadoop.raster.index.merra2;

import java.util.HashMap;
import java.util.List;

import edu.gmu.stc.hadoop.raster.DataChunk;

/**
 * Created by Fei Hu on 3/7/16.
 */
public class Cell {
  List<DataChunk> dataChunkList = null;
  HashMap<String, Integer> hostDistribution = new HashMap<String, Integer>();
}
