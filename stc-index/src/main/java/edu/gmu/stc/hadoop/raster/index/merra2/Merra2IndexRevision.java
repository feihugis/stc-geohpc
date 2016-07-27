package edu.gmu.stc.hadoop.raster.index.merra2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import edu.gmu.stc.database.DBConnector;

/**
 * Created by Fei Hu on 7/13/16.
 */
public class Merra2IndexRevision {
  Statement statement = null;
  String TABLE_PREFIX = "merra2_100_tavg1_2d_int_nx_";

  public Merra2IndexRevision() {
    statement = new DBConnector().GetConnStatement();
  }

  public boolean createTable(String year) {
    String createTableSQL = "CREATE TABLE " + TABLE_PREFIX + year +"_nc4\n"
                            + "(\n"
                            + "  date integer,\n"
                            + "  corner integer[],\n"
                            + "  shape integer[],\n"
                            + "  dimensions text[],\n"
                            + "  filepos integer,\n"
                            + "  bytesize integer,\n"
                            + "  filtermask smallint,\n"
                            + "  hosts text[],\n"
                            + "  datatype text,\n"
                            + "  varshortname text,\n"
                            + "  geometry geometry(Polygon)\n"
                            + ")\n"
                            + "WITH (\n"
                            + "  OIDS=FALSE\n"
                            + ");";
    try {
      System.out.println(createTableSQL);
      statement.execute(createTableSQL);
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public boolean combineTable(String start_year, String end_year) {
    int start_yyyy = Integer.parseInt(start_year), end_yyyy = Integer.parseInt(end_year);
    for (int year = start_yyyy; year<=end_yyyy; year++ ) {
      for (int month = 1; month<=12; month++) {
        for (int day = 1; day<=31; day++) {
          String insertSQL = String.format("Insert into " + TABLE_PREFIX + "%1$04d_nc4(corner,shape,dimensions,filepos,bytesize,filtermask,hosts,datatype,varshortname,geometry) "
                                           + "Select corner,shape,dimensions,filepos,bytesize,filtermask,hosts,datatype,varshortname,geometry from " + TABLE_PREFIX +"%2$04d%3$02d%4$02d_nc4;", year, year, month, day);
          String updateDateSQL = String.format("Update " + TABLE_PREFIX + "%1$04d_nc4 Set date = %2$04d%3$02d%4$02d Where date IS NULL;", year, year, month, day);

          try {
            System.out.println(insertSQL);
            statement.execute(insertSQL);
            System.out.println(updateDateSQL);
            statement.execute(updateDateSQL);
          } catch (SQLException e) {
            //e.printStackTrace();
            System.out.println(e.getSQLState() + " *** error");
            continue;
          }
        }
      }
    }
    return true;
  }

  public static void main(String[] args) {
    Merra2IndexRevision merra2IndexRevision = new Merra2IndexRevision();
    merra2IndexRevision.createTable(args[0]);
    merra2IndexRevision.combineTable(args[0], args[1]);
  }
}
