package edu.gmu.stc.hadoop.raster.index.merra2;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.database.DBConnector;
import edu.gmu.stc.hadoop.raster.ChunkFactory;
import edu.gmu.stc.hadoop.raster.DataChunk;

/**
 * Created by Fei Hu on 2/26/16.
 */
public class Merr2IndexBuilder {
  private Merra2IndexSQL sqlOptor;

  public Merr2IndexBuilder() throws IOException {
    sqlOptor = new Merra2IndexSQL(new DBConnector().GetConnStatement());
  }

  public void initMetaIndexTable() {
    this.sqlOptor.initiateMetaIndexTable();
  }

  /**
   * create tables for file index, tableNames should be lowercase and without "."
   * @param files
   */
  public void createFileIndexTablesInBatch(List<String> files) {
    List<String> tableNames = new ArrayList<String>();
    for (int i=0; i<files.size(); i++) {
      String[] tmps = files.get(i).split("/");
      String tableName = tmps[tmps.length-1].toLowerCase();
      while (tableName.contains(".")) {
        tableName = tableName.replace(".", "_");
      }
      tableNames.add(tableName);
    }
    this.sqlOptor.createFileIndexTablesInBatch(tableNames);
  }

  public void insertMerra2SpaceIndex() {
    this.sqlOptor.insertMerra2SpaceIndex();
  }

  public void insertdataChunks(List<String> files) {
    for (String file : files) {
      Path path = new Path(file);
      String[] tmps = file.split("/");
      String tableName = tmps[tmps.length-1].toLowerCase();
      while (tableName.contains(".")) {
        tableName = tableName.replace(".", "_");
      }
      List<DataChunk> merr2ChunkList = new ArrayList<DataChunk>();
      merr2ChunkList = ChunkFactory.geneDataChunks(path, "nc4");
      this.insertdataChunks(tableName, merr2ChunkList);
    }
  }

  public void insertdataChunks(String tableName, List<DataChunk> chunks) {
    this.sqlOptor.insertDataChunks(tableName, chunks);
  }

  public static void main(String[] args) {
    List<String> files = new ArrayList<String>();
    //files.add("/Users/feihu/Documents/Data/Merra2/MERRA2_100.inst1_2d_int_Nx.19800101.nc4");
    //files.add("/Users/feihu/Documents/Data/Merra2/MERRA2_100.tavg1_2d_int_Nx.19800101.nc4");
    //files.add("/Users/feihu/Documents/Data/Merra2/MERRA2_100.tavg1_2d_int_Nx.19800102.nc4");
    files.add("/Merra/MERRA2/Daily/M2I1NXINT/MERRA2_400.tavg1_2d_int_Nx.20150101.nc4");
    files.add("/Merra/MERRA2/Daily/M2I1NXINT/MERRA2_400.tavg1_2d_int_Nx.20150102.nc4");
    files.add("/Merra/MERRA2/Daily/M2I1NXINT/MERRA2_400.tavg1_2d_int_Nx.20150103.nc4");
    files.add("/Merra/MERRA2/Daily/M2I1NXINT/MERRA2_400.tavg1_2d_int_Nx.20150104.nc4");
    files.add("/Merra/MERRA2/Daily/M2I1NXINT/MERRA2_400.tavg1_2d_int_Nx.20150105.nc4");
    files.add("/Merra/MERRA2/Daily/M2I1NXINT/MERRA2_400.tavg1_2d_int_Nx.20150106.nc4");
    files.add("/Merra/MERRA2/Daily/M2I1NXINT/MERRA2_400.tavg1_2d_int_Nx.20150107.nc4");
    files.add("/Merra/MERRA2/Daily/M2I1NXINT/MERRA2_400.tavg1_2d_int_Nx.20150108.nc4");
    files.add("/Merra/MERRA2/Daily/M2I1NXINT/MERRA2_400.tavg1_2d_int_Nx.20150109.nc4");
    files.add("/Merra/MERRA2/Daily/M2I1NXINT/MERRA2_400.tavg1_2d_int_Nx.20150110.nc4");
    try {
      Merr2IndexBuilder merra2IndexBuilder = new Merr2IndexBuilder();
      merra2IndexBuilder.initMetaIndexTable();
      merra2IndexBuilder.insertMerra2SpaceIndex();
      merra2IndexBuilder.createFileIndexTablesInBatch(files);
      merra2IndexBuilder.insertdataChunks(files);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }





}
