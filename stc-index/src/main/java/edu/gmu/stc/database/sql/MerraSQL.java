package edu.gmu.stc.database.sql;

/**
 * Created by Fei Hu on 1/29/16.
 *
 * This class provide the SQL language for Merra DBIndex Operator
 */
public class MerraSQL {

  static String getCreateTable(String tableName) {
    String sql = "CREATE TABLE \"" + tableName + "\" (" +
                         "\"Time\" integer," +
                         "\"Offset\" integer," +
                         "\"Length\" integer," +
                         "\"BlockHost\" \"varchar\"(300)," +
                         "\"Comp_Code\" integer," +
                         "PRIMARY KEY (\"Time\")" +
                         ")";
    return sql;
  }

  static String getDeleteAllTables(String dbSchema) {
    String sql = "DROP SCHEMA " + dbSchema + " CASCADE;" + "CREATE SCHEMA public;";
    return sql;
  }



}
