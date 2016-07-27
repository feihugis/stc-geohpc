package edu.gmu.stc.hadoop.raster.index.merra2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.configure.MyProperty;
import edu.gmu.stc.database.DBConnector;

/**
 * Created by Fei Hu on 7/11/16.
 */
public class Merra2IndexTest {



  public static void main(String[] args) {
    Merra2IndexSQL sqlOptor;

    String createTableSQL = "CREATE TABLE merra2_100_tavg1_2d_int_nx_1980_nc4\n"
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
                            + "  filepath text,\n"
                            + "  geometry geometry(Polygon)\n"
                            + ")\n"
                            + "WITH (\n"
                            + "  OIDS=FALSE\n"
                            + ");";

    Statement statement = new DBConnector().GetConnStatement();
    try {
      Connection c = statement.getConnection();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    for (int year = 1980; year<=1980; year++ ) {
      for (int month = 1; month<=12; month++) {
        for (int day = 1; day<=31; day++) {
          String insertSQL = String.format("Insert into merra2_100_tavg1_2d_int_nx_%1$04d_nc4(corner,shape,dimensions,filepos,bytesize,filtermask,hosts,datatype,varshortname,filepath,geometry) "
                                           + "Select corner,shape,dimensions,filepos,bytesize,filtermask,hosts,datatype,varshortname,filepath,geometry from merra2_100_tavg1_2d_int_nx_%2$04d%3$02d%4$02d_nc4;", year, year, month, day);
          String updateDateSQL = String.format("Update merra2_100_tavg1_2d_int_nx_%1$04d_nc4 Set date = %2$04d%3$02d%4$02d Where date IS NULL;", year, year, month, day);

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





  }
}
