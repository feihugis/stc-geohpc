package edu.gmu.stc.hadoop.raster.index.merra2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.postgis.PGgeometry;

import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.gmu.stc.configure.MyProperty;
import edu.gmu.stc.database.DBConnector;
import edu.gmu.stc.hadoop.raster.DataChunk;
import edu.gmu.stc.hadoop.raster.RasterUtils;
import edu.gmu.stc.hadoop.raster.hdf5.H5ChunkInputSplit;
import edu.gmu.stc.hadoop.raster.hdf5.H5FileInputFormat;
import edu.gmu.stc.hadoop.raster.hdf5.Merra2Chunk;
import edu.gmu.stc.hadoop.raster.index.MetaData;
import edu.gmu.stc.hadoop.vector.Polygon;

/**
 * Created by Fei Hu on 2/25/16.
 */
public class Merra2IndexSQL {
  private static final Log LOG = LogFactory.getLog(Merra2IndexSQL.class);
  private Statement statement = null;
  private String merra2SpaceIndex = "merra2spaceindex";
  private String optorClass = "merraindex";
  private boolean debug = false;

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

  public void closeDBConection() throws SQLException {
    this.statement.close();
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
   * Create tables for each variable in this kind of product
   * @param varShortNames
   * @param productName
   */
  public void createVarIndexTablesInBatch(List<String> varShortNames, String productName) {
    for (String var : varShortNames) {
      this.createVarIndexTable(productName + "_" + var);
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

  public void createVarIndexTable(String tableName) {
    try {
      String sql;
      sql = "CREATE TABLE public." + tableName + "("
            + "id serial NOT NULL,"
            + "corner integer[],"
            + "date integer,"
            //+ "shape integer[],"
            //+ "dimensions text[],"
            + "filepos integer,"
            + "bytesize integer,"
            //+ "filtermask smallint,"
            + "hosts text[],"
            //+ "datatype text,"
            //+ "varshortname text,"
            //+ "filepath text,"
            + "geometryID smallint,"
            + "CONSTRAINT id" + tableName + " PRIMARY KEY (id)"
            + ");";
      LOG.info( " Create VarIndex table : ************* " + tableName + " ************* by " + sql);
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


  /**
   * Insert Merra2Chunk to table by var
   * @param tableName
   * @param chunks
   */
  public void insertDataChunksByVar(String tableName, List<DataChunk> chunks) {
    String sql = "insert into " + tableName
                 + "(corner, date, filepos, bytesize, hosts, geometryID)"
                 + "values (?,?,?,?,?,?)";
    try {
      Connection c = this.statement.getConnection();
      PreparedStatement ps = c.prepareStatement(sql);
      int count = 0;
      for (DataChunk chk : chunks) {
        Merra2Chunk chunk = new Merra2Chunk(chk.getVarShortName(), chk.getFilePath(), chk.getCorner(),
                                            chk.getShape(), chk.getDimensions(),chk.getFilePos(),
                                            chk.getByteSize(),chk.getFilterMask(),chk.getHosts(),chk.getDataType(), chk.getTime());

        Array corner = c.createArrayOf("integer", RasterUtils.intToInteger(chunk.getCorner()));
        ps.setArray(1, corner);
        ps.setLong(2, chunk.getTime());
        ps.setLong(3, chunk.getFilePos());
        ps.setLong(4, chunk.getByteSize());
        Array hosts = c.createArrayOf("text", chunk.getHosts());
        ps.setArray(5, hosts);
        ps.setInt(6, chunk.getGeometryID());
        count++;
        if ( count%500 == 0 || count == chunks.size()) {
          ps.addBatch();
          ps.executeBatch();
        }
        ps.addBatch();
      }
    } catch (SQLException e) {
      e.printStackTrace();
      e.getNextException().printStackTrace();
    }
  }

  public List<H5ChunkInputSplit> queryDataChunks(List<String> tableNameList, List<String> varList, Polygon polygon) {
    List<H5ChunkInputSplit> inputSplits = new ArrayList<H5ChunkInputSplit>();
    for (String tableName : tableNameList) {
      inputSplits.addAll(queryIntersectedDataChunk(tableName, varList, polygon));
      //inputSplits.addAll(queryContainedDataChunk(tableName, varList, polygon));
    }

    return inputSplits;
  }

  public List<H5ChunkInputSplit> queryDataChunks(List<String> tableNameList,
                                                 List<String> varList,
                                                 Polygon polygon,
                                                 List<Integer[]> starConer,
                                                 List<Integer[]> endCorner) {
    List<H5ChunkInputSplit> inputSplits = new ArrayList<H5ChunkInputSplit>();
    for (String tableName : tableNameList) {
      inputSplits.addAll(queryIntersectedDataChunk(tableName, varList, polygon, starConer, endCorner));
      //inputSplits.addAll(queryContainedDataChunk(tableName, varList, polygon));
    }

    return inputSplits;
  }

  public List<H5ChunkInputSplit> queryDataChunks(List<String> tableNameList,
                                                 List<String> varList,
                                                 Polygon polygon,
                                                 List<Integer[]> starConer,
                                                 List<Integer[]> endCorner,
                                                 int startDate,
                                                 int endDate) {
    List<H5ChunkInputSplit> inputSplits = new ArrayList<H5ChunkInputSplit>();
    for (String tableName : tableNameList) {
      inputSplits.addAll(queryIntersectedDataChunk(tableName, varList, polygon, starConer, endCorner, startDate, endDate));
      //inputSplits.addAll(queryContainedDataChunk(tableName, varList, polygon));
    }

    return inputSplits;
  }

  public List<H5ChunkInputSplit> queryDataChunks(List<String> tableNames,
                                                 List<Integer[]> starConer, List<Integer[]> endCorner,
                                                 int startDate, int endDate,
                                                 String[] geometryIDs) {
    List<H5ChunkInputSplit> inputSplits = new ArrayList<H5ChunkInputSplit>();
    for (String tableName : tableNames) {
      inputSplits.addAll(queryIntersectedDataChunk(tableName, starConer, endCorner, startDate, endDate, geometryIDs));
      //inputSplits.addAll(queryContainedDataChunk(tableName, varList, polygon));
    }

    return inputSplits;

  }


  public List<H5ChunkInputSplit> queryDataChunks(String tableName, List<String> varList, Polygon polygon) {
    List<H5ChunkInputSplit> inputSplits = new ArrayList<H5ChunkInputSplit>();
    inputSplits.addAll(queryIntersectedDataChunk(tableName, varList, polygon));
    //inputSplits.addAll(queryContainedDataChunk(tableName, varList, polygon));
    return inputSplits;
  }

  public List<H5ChunkInputSplit> queryIntersectedDataChunk(String tableName, List<String> varList, Polygon polygon) {
    List<DataChunk> chunkList = new ArrayList<DataChunk>();
    String sql = "SELECT * FROM " + tableName + " AS merra, " + this.merra2SpaceIndex + " AS spaceindex\n"
                  + "WHERE merra.geometry = spaceindex.geometry\n"
                  + "AND ST_Intersects(spaceindex.geometry, '" + polygon.toPostGISPGgeometry().toString()
                  + "'::geometry)\n";
    String varSQL = "AND (";
    for (int i = 0; i < varList.size() - 1; i++) {
      varSQL = varSQL + "merra.varshortname = '" + varList.get(i) + "' \n OR ";
    }

    varSQL = varSQL + "merra.varshortname = '" + varList.get(varList.size() - 1) + "') ORDER BY merra.filepos,merra.corner;";

    sql = sql + varSQL;

    if (debug) {
      LOG.info(sql);
      System.out.println(sql);
    }

    ResultSet rs;
    try {
      rs = this.statement.executeQuery(sql);
      chunkList = generateDataChunk(rs, false); //PostGIS query is ST_Intersects, so the datachunk is not contained in the bbox
    } catch (SQLException e) {
      e.printStackTrace();
    }

    return generateInputSplitByHosts(chunkList);
  }

  public List<H5ChunkInputSplit> queryIntersectedDataChunk(String tableName,
                                                           List<String> varList,
                                                           Polygon polygon,
                                                           List<Integer[]> starConer,
                                                           List<Integer[]> endCorner) {
    List<DataChunk> chunkList = new ArrayList<DataChunk>();
    String sql = "SELECT * FROM " + tableName + " AS merra, " + this.merra2SpaceIndex + " AS spaceindex\n"
                 + "WHERE merra.geometry = spaceindex.geometry\n"
                 + "AND ST_Intersects(spaceindex.geometry, '" + polygon.toPostGISPGgeometry().toString()
                 + "'::geometry)\n";

    String varSQL = "AND (";
    for (int i = 0; i < varList.size(); i++) {
      varSQL = varSQL + "merra.varshortname = '" + varList.get(i) + "' ";
      if (i < varList.size()-1) {
        varSQL = varSQL + " OR ";
      }
    }

    varSQL = varSQL + ") \n";


    /*String cornerSQL = " AND (";

    for (int i = 0; i < starConer.size(); i++ ) {
      Integer[] subStart = starConer.get(i);
      Integer[] subEnd = endCorner.get(i);
      String subCornerSQL = "(";
      for (int j = 0; j < subStart.length; j++) {
        subCornerSQL = subCornerSQL + " ( merra.corner["+(j+1)+"] >=" + subStart[j] + " AND merra.corner["+(j+1)+"] <=" + subEnd[j] + ")";
        if (j < subStart.length-1) {
          subCornerSQL = subCornerSQL + "AND ";
        }
      }
      subCornerSQL = subCornerSQL + " )";
      cornerSQL = cornerSQL + subCornerSQL;
      if (i<starConer.size()-1) {
        cornerSQL = cornerSQL + " OR ";
      }
    }

    cornerSQL = cornerSQL + " )\n";*/


    String orderSQL = "ORDER BY merra.filepos,merra.corner;";

    sql = sql + varSQL + orderSQL;
    //sql = sql + varSQL + cornerSQL + orderSQL;

    if (debug) {
      LOG.info(sql);
    }

    ResultSet rs;
    try {
      rs = this.statement.executeQuery(sql);
      chunkList = generateDataChunk(rs, false); //PostGIS query is ST_Intersects, so the datachunk is not contained in the bbox
    } catch (SQLException e) {
      e.printStackTrace();
    }

    return generateInputSplitByHosts(chunkList);
  }

  public List<H5ChunkInputSplit> queryIntersectedDataChunk(String tableName,
                                                           List<String> varList,
                                                           Polygon polygon,
                                                           List<Integer[]> starConer,
                                                           List<Integer[]> endCorner,
                                                           int startDate,
                                                           int endDate) {
    List<DataChunk> chunkList = new ArrayList<DataChunk>();
    String sql = "SELECT * FROM " + tableName + " AS merra \n"
                 + "WHERE date >= " + startDate + " AND date <= " + endDate + "\n"
                 + "AND ST_Intersects(merra.geometry, '" + polygon.toPostGISPGgeometry().toString()
                 + "'::geometry)\n";

    String varSQL = "AND (";
    for (int i = 0; i < varList.size(); i++) {
      varSQL = varSQL + "merra.varshortname = '" + varList.get(i) + "' ";
      if (i < varList.size()-1) {
        varSQL = varSQL + " OR ";
      }
    }

    varSQL = varSQL + ") \n";


    /*String cornerSQL = " AND (";

    for (int i = 0; i < starConer.size(); i++ ) {
      Integer[] subStart = starConer.get(i);
      Integer[] subEnd = endCorner.get(i);
      String subCornerSQL = "(";
      for (int j = 0; j < subStart.length; j++) {
        subCornerSQL = subCornerSQL + " ( merra.corner["+(j+1)+"] >=" + subStart[j] + " AND merra.corner["+(j+1)+"] <=" + subEnd[j] + ")";
        if (j < subStart.length-1) {
          subCornerSQL = subCornerSQL + "AND ";
        }
      }
      subCornerSQL = subCornerSQL + " )";
      cornerSQL = cornerSQL + subCornerSQL;
      if (i<starConer.size()-1) {
        cornerSQL = cornerSQL + " OR ";
      }
    }

    cornerSQL = cornerSQL + " )\n";*/


    String orderSQL = "ORDER BY merra.date,merra.filepos,merra.corner;";

    sql = sql + varSQL + orderSQL;
    //sql = sql + varSQL + cornerSQL + orderSQL;

    if (debug) {
      LOG.info(sql);
    }

    ResultSet rs;
    try {
      rs = this.statement.executeQuery(sql);
      LOG.info("++++++++++++++++++++ finish sql");
      chunkList = generateDataChunk(rs, false); //PostGIS query is ST_Intersects, so the datachunk is not contained in the bbox
    } catch (SQLException e) {
      e.printStackTrace();
    }

    LOG.info("******************** chunk size : " + chunkList.size());
    return generateInputSplitByHosts(chunkList);
  }

  public List<H5ChunkInputSplit> queryIntersectedDataChunk(String tableName,
                                                           List<Integer[]> starConer,
                                                           List<Integer[]> endCorner,
                                                           int startDate,
                                                           int endDate,
                                                           String[] geometryIDs) {
    List<DataChunk> chunkList = new ArrayList<DataChunk>();
    String var = tableName.split("_")[1];

    String sql = "SELECT * FROM " + tableName + " AS merra \n"
                 + "WHERE ";

    String geometrySQL = "";
    for (int i = 0; i < geometryIDs.length; i++) {
      geometrySQL = geometrySQL + "merra.geometryid = " + geometryIDs[i] + " ";
      if (i < geometryIDs.length - 1) {
        geometrySQL = geometrySQL + " OR ";
      }
    }

    geometrySQL = geometrySQL + " \n";

    String dateSQL = "AND (";
    dateSQL = dateSQL + "date >= " + startDate + " AND date <= " + endDate + ") \n";

    /*String cornerSQL = " AND (";

    for (int i = 0; i < starConer.size(); i++ ) {
      Integer[] subStart = starConer.get(i);
      Integer[] subEnd = endCorner.get(i);
      String subCornerSQL = "(";
      for (int j = 0; j < subStart.length; j++) {
        subCornerSQL = subCornerSQL + " ( merra.corner["+(j+1)+"] >=" + subStart[j] + " AND merra.corner["+(j+1)+"] <=" + subEnd[j] + ")";
        if (j < subStart.length-1) {
          subCornerSQL = subCornerSQL + "AND ";
        }
      }
      subCornerSQL = subCornerSQL + " )";
      cornerSQL = cornerSQL + subCornerSQL;
      if (i<starConer.size()-1) {
        cornerSQL = cornerSQL + " OR ";
      }
    }

    cornerSQL = cornerSQL + " )\n";*/


    String orderSQL = "ORDER BY merra.date,merra.filepos,merra.corner;";

    sql = sql + geometrySQL + orderSQL;
    //sql = sql + varSQL + cornerSQL + orderSQL;

    LOG.info(sql);

    if (debug) {
      LOG.info(sql);
    }

    ResultSet rs;
    try {
      rs = this.statement.executeQuery(sql);
      LOG.info("++++++++++++++++++++ finish sql");
      chunkList = generateDataChunk(rs, var, false); //PostGIS query is ST_Intersects, so the datachunk is not contained in the bbox
    } catch (SQLException e) {
      e.printStackTrace();
    }

    LOG.info("******************** chunk size : " + chunkList.size());
    return generateInputSplitByHosts(chunkList);
  }

  public List<H5ChunkInputSplit> queryContainedDataChunk(String tableName, List<String> varList, Polygon polygon) {
    List<DataChunk> chunkList = new ArrayList<DataChunk>();
    String sql = "SELECT * FROM " + tableName + " AS merra, " + this.merra2SpaceIndex + " AS spaceindex\n"
                 + "WHERE merra.geometry = spaceindex.geometry\n"
                 + "AND ST_Contains('" + polygon.toPostGISPGgeometry().toString() + "'::geometry" + ", spaceindex.geometry )\n";
    String varSQL = "AND (";
    for (int i = 0; i < varList.size() - 1; i++) {
      varSQL = varSQL + "merra.varshortname = '" + varList.get(i) + "' \n OR ";
    }

    varSQL = varSQL + "merra.varshortname = '" + varList.get(varList.size() - 1) + "') ORDER BY merra.filepos,merra.corner;";

    sql = sql + varSQL;

    if (debug) LOG.info(sql);

    ResultSet rs;
    try {
      rs = this.statement.executeQuery(sql);
      chunkList = generateDataChunk(rs, true);  //PostGIS query is ST_Contains, so the datachunk is contained in the bbox
    } catch (SQLException e) {
      e.printStackTrace();
    }

    /*for (DataChunk dataChunk : chunkList) {
      LOG.info("+++++++++++++++   " +  dataChunk.getFilePath());
    }*/
    return generateInputSplitByHosts(chunkList);
  }

  public List<H5ChunkInputSplit> generateInputSplitByHosts(List<DataChunk> inputChunkList) {
    List<H5ChunkInputSplit> inputSplitList = new ArrayList<H5ChunkInputSplit>();
    List<DataChunk> chunkList = new ArrayList<DataChunk>();
    inputChunkList.add(null);
    for(int i=0; i<inputChunkList.size()-1; i++) {
      chunkList.add(inputChunkList.get(i));
      if (RasterUtils.isShareHosts(inputChunkList.get(i), inputChunkList.get(i+1))
          && inputChunkList.get(i).getFilePath().equals(inputChunkList.get(i+1).getFilePath())) {
        continue;
      } else {
        inputSplitList.add(new H5ChunkInputSplit(chunkList));
        chunkList = new ArrayList<DataChunk>();
      }
    }
    return inputSplitList;
  }

 public List<DataChunk> generateDataChunk(ResultSet rs, boolean isContain) {
    List<DataChunk> chunkList = new ArrayList<DataChunk>();
    try {
      rs.last();
      if (rs.getRow() == 0) {
        System.out.println(this.getClass().getName() + ".generateDataChunk: ResultSet is empty");
        rs.close();
      } else {
        rs.first();
        do {
          Integer[] corner = (Integer[]) rs.getArray("corner").getArray();
          Integer[] shape = (Integer[]) rs.getArray("shape").getArray();
          String[] dimensions = (String[]) rs.getArray("dimensions").getArray();
          Long filepos = rs.getLong("filepos");
          Long bytesize = rs.getLong("bytesize");
          int filtermask = rs.getInt("filtermask");
          String[] hosts = (String[]) rs.getArray("hosts").getArray();
          String datatype = rs.getString("datatype");
          String varshortname = rs.getString("varshortname");
          String date = rs.getString("date");
          String year = date.substring(0,4);
          String month = date.substring(4,6);
          String day = date.substring(6,8);
          String filepath = MyProperty.HDFS_FilePATH_PREFIX + year + "/" + month + "/"
                            + MyProperty.MERRA2_FILE_PREFIX + date + MyProperty.MERRA2_FILE_POSTFIX;

          Merra2Chunk merra2Chunk = new Merra2Chunk(varshortname, filepath, RasterUtils.IntegerToint(corner), RasterUtils.IntegerToint(shape),
                                                    dimensions, filepos, bytesize, filtermask,
                                                    hosts,datatype);
          merra2Chunk.setContain(isContain);
          chunkList.add(merra2Chunk);
          rs.next();
        } while (!rs.isAfterLast());
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }

    return chunkList;
  }


  public List<DataChunk> generateDataChunk(ResultSet rs, String varshortname, boolean isContain) {
    List<DataChunk> chunkList = new ArrayList<DataChunk>();
    try {
      rs.last();
      if (rs.getRow() == 0) {
        System.out.println(this.getClass().getName() + ".generateDataChunk: ResultSet is empty");
        rs.close();
      } else {
        rs.first();
        do {
          Integer[] corner = (Integer[]) rs.getArray("corner").getArray();
          Integer[] shape = new Integer[] {1, 91, 144};
          String[] dimensions = new String[] {"time", "lat", "lon"};
          Long filepos = rs.getLong("filepos");
          Long bytesize = rs.getLong("bytesize");
          int filtermask = 0; //rs.getInt("filtermask");
          String[] hosts = (String[]) rs.getArray("hosts").getArray();
          String datatype = "float"; //rs.getString("datatype");
          //String varshortname = rs.getString("varshortname");
          String date = rs.getString("date");
          int time = Integer.parseInt(date);
          String year = date.substring(0,4);
          String month = date.substring(4,6);
          String day = date.substring(6,8);
          int yyyy = time/10000; //Integer.parseInt(year);

          //TODO: filepath need be used in the chunk combination, so it could not be null
          String filepath = "" + time;

          //TODO: even for the same product, it may have different name rule.
          /*if (yyyy >= 1992) {
            filepath = MyProperty.HDFS_FilePATH_PREFIX + year + "/" + month + "/"
                       + MyProperty.MERRA2_FILE_PREFIX_Sec + date + MyProperty.MERRA2_FILE_POSTFIX;
          } else {
            filepath = MyProperty.HDFS_FilePATH_PREFIX + year + "/" + month + "/"
                       + MyProperty.MERRA2_FILE_PREFIX + date + MyProperty.MERRA2_FILE_POSTFIX;
          }*/

          Merra2Chunk merra2Chunk = new Merra2Chunk(varshortname, filepath, RasterUtils.IntegerToint(corner), RasterUtils.IntegerToint(shape),
                                                    dimensions, filepos, bytesize, filtermask,
                                                    hosts,datatype, time);
          merra2Chunk.setContain(isContain);
          chunkList.add(merra2Chunk);
          rs.next();
        } while (!rs.isAfterLast());
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }

    return chunkList;
  }


  public static void main(String[] args) {
    Merra2IndexSQL hdf5IdxBuilder = new Merra2IndexSQL(new DBConnector().GetConnStatement());
    List<String> varList = new ArrayList<String>();
    varList.add("UFLXKE");
    varList.add("AUTCNVRN");
    varList.add("BKGERR");
    List<String> files = new ArrayList<String>();
    files.add("merra2_400_tavg1_2d_int_nx_20150101_nc4");
    files.add("merra2_400_tavg1_2d_int_nx_20150102_nc4");
    files.add("merra2_400_tavg1_2d_int_nx_20150103_nc4");
    files.add("merra2_400_tavg1_2d_int_nx_20150104_nc4");
    files.add("merra2_400_tavg1_2d_int_nx_20150105_nc4");
    files.add("merra2_400_tavg1_2d_int_nx_20150106_nc4");
    files.add("merra2_400_tavg1_2d_int_nx_20150107_nc4");
    files.add("merra2_400_tavg1_2d_int_nx_20150108_nc4");
    files.add("merra2_400_tavg1_2d_int_nx_20150109_nc4");
    Polygon plgn = new Polygon(new double[]{0.0, 1.0, 1.0, 0.0}, new double[]{0.0, 0.0,1.0,1.0}, 4);
    //hdf5IdxBuilder.queryDataChunks("merra2_100_tavg1_2d_int_nx_19800101_nc4", varList, plgn);
    hdf5IdxBuilder.queryDataChunks(files, varList, plgn);
    //hdf5IdxBuilder.createPostGISExtension();
    //hdf5IdxBuilder.createMerra2SpaceIndexTable();
    //hdf5IdxBuilder.createFileIndexTable("MERRA2_100_inst1_2d_int_Nx_222129800107_Nc4");
    /*List<String> names = new ArrayList<String>();
    for (int i=0; i< 5; i++) {
      names.add("test"+i);
    }*/
    //hdf5IdxBuilder.createBtreeOperatorClass();
    //hdf5IdxBuilder.addBtreeIndex2Table("lai", "geometry");
    //hdf5IdxBuilder.addBtreeIndex2Table("merra2spaceindex", "geometry");
    //hdf5IdxBuilder.addForeignKey("lai");

/*    try {
      NetcdfFile nc = NetcdfFile.open("/Users/feihu/Desktop/MOP01-20000319-L1V3.42.0.he5");
      List<Variable> variableList = nc.getVariables();
      for (Variable var : variableList) {
        System.out.println(var.read().toString());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }*/
  }
}
