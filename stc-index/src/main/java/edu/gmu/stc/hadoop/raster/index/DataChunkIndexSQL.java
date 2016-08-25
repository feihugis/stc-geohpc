package edu.gmu.stc.hadoop.raster.index;

import java.sql.Statement;
import java.util.List;

import edu.gmu.stc.hadoop.raster.DataChunk;

/**
 * Created by Fei Hu on 8/24/16.
 */
public abstract class DataChunkIndexSQL {

  public abstract void createTable(String tableName, Statement statement);

  public abstract void interDataChunks(List<DataChunk> chunkList, Statement statement);

  public abstract void dropTable(String tableName, Statement statement);
}
