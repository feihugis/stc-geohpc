package edu.gmu.stc.hadoop.raster.index;

import com.esotericsoftware.kryo.KryoSerializable;

import org.apache.hadoop.io.Writable;
import org.apache.spark.serializer.KryoSerializer;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import edu.gmu.stc.hadoop.raster.DataChunk;

/**
 * Created by Fei Hu on 8/25/16.
 */
public interface DataChunkIndexBuilder extends Writable{

  public void createTable(String tableName, Statement statement);

  public void insertDataChunks(List<DataChunk> chunkList, String tablename, Statement statement);

  public List<DataChunk> queryDataChunks(String tableName, int startTime, int endTime, String[] geometryInfos, Statement statement);

  public String abbreviateFilePath(String filePath, String prefix, String suffix);

  public String expandFilePath(String filePath, String prefix, String suffix);

  public List<DataChunk> convertRSToDataChunk(ResultSet rs);

  public void dropTable(String tableName, Statement statement);

}
