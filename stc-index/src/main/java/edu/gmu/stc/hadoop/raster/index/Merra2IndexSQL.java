package edu.gmu.stc.hadoop.raster.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.postgis.Geometry;
import org.postgis.PGgeometry;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.gmu.stc.database.DBConnector;
import edu.gmu.stc.hadoop.raster.DataChunk;
import edu.gmu.stc.hadoop.raster.RasterUtils;
import edu.gmu.stc.hadoop.raster.hdf5.Merra2Chunk;
import edu.gmu.stc.hadoop.vector.Polygon;

/**
 * Created by Fei Hu on 2/25/16.
 */
public class Merra2IndexSQL {
  private static final Log LOG = LogFactory.getLog(Merra2IndexSQL.class);
  private Statement statement = null;
  private String merra2SpaceIndex = "merra2spaceindex";
  private String optorClass = "merraindex";

  public Merra2IndexSQL(Statement statement) {
    this.statement = statement;
  }

  /**
   * Add postgis extension, and initiate the 2D index table for Merra2 MetaData, e.g. lat, lon
   */
  public void initiateMetaIndexTable() {
    this.createPostGISExtension();
    this.createMerra2SpaceIndexTable();
    this.createBtreeOperatorClass();
    this.addBtreeIndex2Table(this.merra2SpaceIndex, "geometry");
  }

  /**
   * Create tables for the file index
   * @param tablenames  the table name should be same with the file name
   */
  public void createFileIndexTablesInBatch(List<String> tablenames) {
    for(String table: tablenames) {
      this.createFileIndexTable(table);
      this.addBtreeIndex2Table(table, "geometry");
      this.addForeignKey(table);
    }
  }

  /**
   * Create PostGIS extentsion for PostgreSQL
   */
  public void createPostGISExtension() {
    try {
      String sql;
      sql = "CREATE EXTENSION postgis;";
      LOG.info( " Create Extension : ************* PostGIS ************* by " + sql);
      this.statement.execute(sql);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * Create the space index table, and will save the 2D chunk info here by the Merra2 metadata
   */
  public void createMerra2SpaceIndexTable() {
    try {
      String sql;
      sql = "CREATE TABLE public." + merra2SpaceIndex + "("
            + "tid serial NOT NULL,"
            + "corner integer[],"
            + "shape integer[],"
            + "geometry geometry(POLYGON) NOT NULL,"
            + "CONSTRAINT geometry PRIMARY KEY (geometry));";
      LOG.info( " Create table : ************* " + merra2SpaceIndex + " ************* by " + sql);
      this.statement.execute(sql);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * Create table for Variable index
   * @param tableName table name. For merra2, it should be the filename by lower case.
   */
  public void createFileIndexTable(String tableName) {
    try {
      tableName = tableName;

      String sql;
      sql = "CREATE TABLE public." + tableName + "("
            + "id serial NOT NULL,"
            + "corner integer[],"
            + "shape integer[],"
            + "dimensions text[],"
            + "filepos integer,"
            + "bytesize integer,"
            + "filtermask smallint,"
            + "hosts text[],"
            + "datatype text,"
            + "varshortname text,"
            + "filepath text,"
            + "geometry geometry(POLYGON),"
            + "CONSTRAINT id" + tableName + " PRIMARY KEY (id)"
            + ");";
      LOG.info( " Create FileIndex table : ************* " + tableName + " ************* by " + sql);
      this.statement.execute(sql);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * Create Operator Class, which will be used to build the b-tree index for the table column
   */
  public void createBtreeOperatorClass() {
    try {
      String sql;
      sql = "CREATE OPERATOR CLASS public." + optorClass + " FOR TYPE polygon"
            + " USING btree AS "
            + " STORAGE polygon;";
      LOG.info( " Create OPERATOR CLASS : ************* " + optorClass + " ************* by " + sql);
      this.statement.execute(sql);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * Use B-tree operator class to build b-tree for the geometry column
   * @param table
   */
  public void addBtreeIndex2Table(String table, String column) {
    try {
      String sql;
      sql = "CREATE INDEX " + optorClass + table + " ON public." + table
            + " USING btree"
            + "("+ column + " ASC NULLS LAST);";
      LOG.info( " Create Btree Index for Table : ************* " + table + " ************* by " + sql);
      this.statement.execute(sql);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * Add forgein key between variable table and merra2spaceindex table
   * @param table
   */
  public void addForeignKey(String table) {
    try {
      String sql;
      sql = "ALTER TABLE public." + table + " ADD CONSTRAINT " + table + "geometry FOREIGN KEY (geometry)"
            + "REFERENCES public." + merra2SpaceIndex + " (geometry) MATCH FULL"
            + " ON DELETE NO ACTION ON UPDATE NO ACTION;";
      LOG.info( " Add forgein key for Table : ************* " + table + " ************* by " + sql);
      this.statement.execute(sql);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * build the spatial index, but the shape info for the variables time, lat, lon is not right
   */
  public void insertMerra2SpaceIndex() {
    String sql = "insert into " + this.merra2SpaceIndex
                 + "(corner, shape, geometry) "
                 + "values (?,?,?)";
    HashMap<int[], Polygon> chunkBoundaries = MetaData.MERRA2.getChunkBoundaries();
    int[] chunkShape = new int[] {MetaData.MERRA2.latChunkShape, MetaData.MERRA2.lonChunkShape};
    try {
      Connection c = this.statement.getConnection();
      PreparedStatement ps = c.prepareStatement(sql);
      Iterator<Map.Entry<int[], Polygon>> itor = chunkBoundaries.entrySet().iterator();
      while (itor.hasNext()) {
        Map.Entry<int[], Polygon> entry = itor.next();
        Array corner = c.createArrayOf("integer", RasterUtils.intToInteger(entry.getKey()));
        ps.setArray(1, corner);
        Array shape = c.createArrayOf("integer", RasterUtils.intToInteger(chunkShape));
        ps.setArray(2, shape);
        Polygon polygon = entry.getValue();
        ps.setObject(3, polygon.toPostGISPGgeometry());
        ps.addBatch();
      }
      ps.executeBatch();
    } catch (SQLException e) {
      e.printStackTrace();
      e.getNextException().printStackTrace();
    }
  }

  /**
   * Insert Merra2Chunk to table
   * @param tableName
   * @param chunks
   */
  public void insertDataChunks(String tableName, List<DataChunk> chunks) {
    String sql = "insert into " + tableName
                 + "(corner, shape, dimensions, filepos, bytesize, filtermask, "
                 + "hosts, datatype, varshortname, filepath, geometry) "
                 + "values (?,?,?,?,?,?,?,?,?,?,?)";
    try {
      Connection c = this.statement.getConnection();
      PreparedStatement ps = c.prepareStatement(sql);
      int count = 0;
      for (DataChunk chk : chunks) {
         Merra2Chunk chunk = new Merra2Chunk(chk.getVarShortName(), chk.getFilePath(), chk.getCorner(),
                                            chk.getShape(), chk.getDimensions(),chk.getFilePos(),
                                            chk.getByteSize(),chk.getFilterMask(),chk.getHosts(),chk.getDataType());

        Array corner = c.createArrayOf("integer", RasterUtils.intToInteger(chunk.getCorner()));
        ps.setArray(1, corner);
        Array shape = c.createArrayOf("integer", RasterUtils.intToInteger(chunk.getShape()));
        ps.setArray(2, shape);
        Array dimensions = c.createArrayOf("text", chunk.getDimensions());
        ps.setArray(3, dimensions);
        ps.setLong(4, chunk.getFilePos());
        ps.setLong(5, chunk.getByteSize());
        ps.setInt(6, chunk.getFilterMask());
        Array hosts = c.createArrayOf("text", chunk.getHosts());
        ps.setArray(7, hosts);
        ps.setString(8, chunk.getDataType());
        ps.setString(9, chunk.getVarShortName());
        ps.setString(10, chunk.getFilePath());

        Polygon polygon = chunk.getBoundary();
        ps.setObject(11, polygon.toPostGISPGgeometry());
        count++;
        if ( count%500 == 0 || count == chunks.size()) {
          ps.executeBatch();
        }
        ps.addBatch();
      }
    } catch (SQLException e) {
      e.printStackTrace();
      e.getNextException().printStackTrace();
    }

  }

  public static void main(String[] args) {
    Merra2IndexSQL hdf5IdxBuilder = new Merra2IndexSQL(new DBConnector().GetConnStatement());
    //hdf5IdxBuilder.createPostGISExtension();
    //hdf5IdxBuilder.createMerra2SpaceIndexTable();
    //hdf5IdxBuilder.createFileIndexTable("MERRA2_100_inst1_2d_int_Nx_222129800107_Nc4");
    List<String> names = new ArrayList<String>();
    for (int i=0; i< 5; i++) {
      names.add("test"+i);
    }
    //hdf5IdxBuilder.createBtreeOperatorClass();
    //hdf5IdxBuilder.addBtreeIndex2Table("lai", "geometry");
    //hdf5IdxBuilder.addBtreeIndex2Table("merra2spaceindex", "geometry");
    //hdf5IdxBuilder.addForeignKey("lai");
  }
}
