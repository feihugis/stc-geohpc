package edu.gmu.stc.hadoop.raster.index;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.database.DBConnector;
import edu.gmu.stc.hadoop.raster.ChunkUtils;
import edu.gmu.stc.hadoop.raster.DataChunk;
import edu.gmu.stc.hadoop.raster.RasterUtils;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

/**
 * Created by Fei Hu on 8/24/16.
 */
public class DataChunkIndexBuilderImp implements DataChunkIndexBuilder{
  private static final Log LOG = LogFactory.getLog(DataChunkIndexBuilderImp.class);

  private String file_prefix = "";
  private String file_suffix = "";

  public DataChunkIndexBuilderImp() {

  }

  /**
   * Need file naming rule pattern to save the index size
   * @param file_prefix
   * @param file_suffix
   */
  public DataChunkIndexBuilderImp(String file_prefix, String file_suffix) {
    this.file_prefix = file_prefix;
    this.file_suffix = file_suffix;
  }

  @Override
  public void createDatabase(String databaseName, String userName, Statement statement) {
    String create_database_SQL;
    create_database_SQL = "CREATE DATABASE " + databaseName + " WITH OWNER = " + userName;

    LOG.info("++++++++++  Create Database Index Table by " + " ---" + create_database_SQL);

    try {
      statement.execute(create_database_SQL);
    } catch (SQLException e) {
      LOG.error(e.toString());
      e.printStackTrace();
    }

  }

  public void createTable(String tableName, Statement statement) {
    String create_table_SQL;
    create_table_SQL = "CREATE TABLE public." + tableName + "("
                       + "id serial NOT NULL,"
                       + "varshortname character varying(255),"
                       + "corner integer[],"
                       + "shape integer[],"
                       + "dimensions text[],"
                       + "time integer,"
                       + "datatype character varying(255),"
                       + "filepos integer,"
                       + "bytesize integer,"
                       + "filtermask smallint,"
                       + "hosts text[],"
                       + "filepath character varying(255),"
                       + "geometryInfo character varying(255),"
                       + "CONSTRAINT id" + tableName + " PRIMARY KEY (id)"
                       + ");";

    LOG.info("++++++++++  Create DataChunk Index Table by " + " ---" + create_table_SQL);

    try {
      statement.execute(create_table_SQL);
    } catch (SQLException e) {
      LOG.error(e.toString());
      e.printStackTrace();
    }
  }

  /**
   * TODO: here we assume that the chunk list have the same variable, and the table name is for this varaible.
   * @param chunkList
   * @param tablename
   * @param statement
   */
  public void insertDataChunks(List<DataChunk> chunkList, String tablename, Statement statement) {
    String insert_DataChunk_SQL = "insert into " + tablename + "(varshortname, corner, shape, dimensions, time, datatype,"
                                  + "filepos, bytesize, filtermask, hosts, filepath, geometryInfo) "
                                  + "values (?,?,?,?,?,?,?,?,?,?,?,?)";
    try {
      Connection connection = statement.getConnection();
      PreparedStatement preparedStatement = connection.prepareStatement(insert_DataChunk_SQL);
      int count = 0;
      for (DataChunk chk : chunkList) {
        preparedStatement.setString(1, chk.getVarShortName());

        Array corner = connection.createArrayOf("integer", RasterUtils.intToInteger(chk.getCorner()));
        preparedStatement.setArray(2, corner);

        Array shape = connection.createArrayOf("integer", RasterUtils.intToInteger(chk.getShape()));
        preparedStatement.setArray(3, shape);

        Array dims = connection.createArrayOf("text", chk.getDimensions());
        preparedStatement.setArray(4, dims);

        preparedStatement.setInt(5, chk.getTime());
        preparedStatement.setString(6, chk.getDataType());
        preparedStatement.setLong(7, chk.getFilePos());
        preparedStatement.setLong(8, chk.getByteSize());
        preparedStatement.setInt(9, chk.getFilterMask());

        Array hosts = connection.createArrayOf("text", chk.getHosts());
        preparedStatement.setArray(10, hosts);

        String filePath = abbreviateFilePath(chk.getFilePath(), file_prefix, file_suffix);
        preparedStatement.setString(11, filePath);

        preparedStatement.setString(12, chk.getGeometryInfo());

        count++;
        preparedStatement.addBatch();

        if (count%1000 == 0 || count == chunkList.size()) {
          preparedStatement.executeBatch();
        }

      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * TODO: here use "+" and "-" to replace the pattern part, is there any better way?
   * @param filePath
   * @param prefix
   * @param suffix
   * @return
   */
  public String abbreviateFilePath(String filePath, String prefix, String suffix) {
    filePath = filePath.replace(prefix, "+");
    filePath = filePath.replace(suffix, "!");
    return filePath;
  }

  /**
   * TODO: here use "+" and "-" to replace the pattern part, is there any better way?
   * @param filePath
   * @param prefix
   * @param suffix
   * @return
   */
  public String expandFilePath(String filePath, String prefix, String suffix) {
    filePath = filePath.replace("+", prefix);
    filePath = filePath.replace("!", suffix);
    return  filePath;
  }

  public List<DataChunk> queryDataChunks(String tableName, int startTime, int endTime, String[] geometryInfos, Statement statement) {
    String query_dataChunk_SQL = String.format("SELECT * FROM %1$s WHERE time >= %2$d AND time <= %3$d AND ", tableName, startTime, endTime);
    String geometry_SQL = "(";
    int geoNum = geometryInfos.length;
    for (int i=0; i<geoNum; i++) {
      geometry_SQL = geometry_SQL + "geometryInfo = '" + geometryInfos[i] + "'";
      if (i < geoNum -1) {
        geometry_SQL = geometry_SQL + " OR ";
      } else {
        geometry_SQL = geometry_SQL + ")";
      }
    }


    query_dataChunk_SQL = query_dataChunk_SQL + geometry_SQL;

    String order_SQL = " ORDER BY time, filepos, corner;";
    query_dataChunk_SQL = query_dataChunk_SQL + order_SQL;

    //LOG.info("++++++++++ Query Table " + tableName + " by SQL --- " + query_dataChunk_SQL);

    List<DataChunk> dataChunkList = new ArrayList<DataChunk>();

    try {
      ResultSet resultSet = statement.executeQuery(query_dataChunk_SQL);
      //LOG.info("++++++++++ Finish Query");
      dataChunkList = convertRSToDataChunk(resultSet);
      return dataChunkList;
    } catch (SQLException e) {
      e.printStackTrace();
      //new Exception(query_dataChunk_SQL);
    }

    return null;
  }

  public List<DataChunk> convertRSToDataChunk(ResultSet rs) {
    List<DataChunk> chunkList = new ArrayList<DataChunk>();
    try {
      rs.last();
      if (rs.getRow() == 0) {
        //LOG.debug(this.getClass().getName() + ".generateDataChunk: ResultSet is empty");
        rs.close();
      } else {
        rs.first();
        do {
          String varshortname = rs.getString("varshortname");
          Integer[] corner = (Integer[]) rs.getArray("corner").getArray();
          Integer[] shape = (Integer[]) rs.getArray("shape").getArray();
          String[] dimensions = (String[]) rs.getArray("dimensions").getArray();
          String date = rs.getString("time");
          int time = Integer.parseInt(date);
          String datatype = rs.getString("datatype");
          Long filepos = rs.getLong("filepos");
          Long bytesize = rs.getLong("bytesize");
          int filtermask = rs.getInt("filtermask");
          String[] hosts = (String[]) rs.getArray("hosts").getArray();
          //TODO: filepath need be used in the chunk combination, so it could not be null
          String filepath = rs.getString("filepath");
          filepath = expandFilePath(filepath, file_prefix, file_suffix);

          String geometryInfo = rs.getString("geometryInfo");

          DataChunk dataChunk = new DataChunk(RasterUtils.IntegerToint(corner), RasterUtils.IntegerToint(shape), dimensions,
                                              filepos, bytesize, filtermask, hosts, datatype, varshortname, filepath, time, geometryInfo);
          chunkList.add(dataChunk);
          rs.next();
        } while (!rs.isAfterLast());
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }

    return chunkList;
  }

  public void dropTable(String tableName, Statement statement) {
    String dropTable_SQL = String.format("DROP TABLE %1$s;", tableName);
    try {
      statement.execute(dropTable_SQL);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void dropDatabase(String databaseName, Statement statement) {

  }

  public String getFile_prefix() {
    return file_prefix;
  }

  public void setFile_prefix(String file_prefix) {
    this.file_prefix = file_prefix;
  }

  public String getFile_suffix() {
    return file_suffix;
  }

  public void setFile_suffix(String file_suffix) {
    this.file_suffix = file_suffix;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, this.file_prefix);
    Text.writeString(out, this.file_suffix);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.file_prefix = Text.readString(in);
    this.file_suffix = Text.readString(in);
  }

  //@Override
  public void write(Kryo kryo, Output output) {
    //output.writeString(this.file_prefix);
    //output.writeString(this.file_suffix);
    kryo.writeClassAndObject(output, this.file_prefix);
    kryo.writeClassAndObject(output, this.file_suffix);
  }

  //@Override
  public void read(Kryo kryo, Input input) {
    //this.file_prefix = input.readString();
    //this.file_suffix = input.readString();
    this.file_prefix = (String) kryo.readClassAndObject(input);
    this.file_suffix = (String) kryo.readClassAndObject(input);
  }
}
