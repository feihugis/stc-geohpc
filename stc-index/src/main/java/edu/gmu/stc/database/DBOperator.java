package edu.gmu.stc.database;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.hadoop.index.io.merra.VariableInfo;


/**
 * @author feihu
 *
 */
public class DBOperator {
	private static final Log LOG = LogFactory.getLog(DBOperator.class);
	
	private Statement statement = null;
	
	public DBOperator(Statement statement) {
		this.statement = statement;
	}
	
	public void createTable(String tableName) {
		String sql;
		/*sql = "CREATE TABLE `MerraIndex`.`" + tableName + "` ("
				+ "`Time` INT(11) NOT NULL,"
				+ "`Offset` INT(11) NULL,"
				+ "`Length` INT(11) NULL,"
				+ "`BlockHost` VARCHAR(300) NULL,"
				+ "`Comp_Code` INT NULL,"
				+ "PRIMARY KEY (`Time`))" ;*/	
		sql = "CREATE TABLE \"" + tableName + "\" (" +
				       "\"Time\" integer," + 
				       "\"Offset\" integer," + 
				       "\"Length\" integer," + 
				       "\"BlockHost\" \"varchar\"(300)," + 
				       "\"Comp_Code\" integer," +
				       "PRIMARY KEY (\"Time\")" + 
						")";
				//+ "PRIMARY KEY (\"Time\"))" ;
		System.out.println(sql);
		try {
			this.statement.execute(sql);
			System.out.printf("Create table: %s \n", tableName);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void deleteAllTables(String schema) {
		String sql;
		sql = "DROP SCHEMA " + schema + " CASCADE;" + "CREATE SCHEMA public;";
		System.out.println(sql);
		try {
			this.statement.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void addVarRecordList(List<VariableInfo> varInfoList, String dateType) {
		
		String values = "";
		String varShortName = varInfoList.get(0).getShortName();
		for(VariableInfo varInfo : varInfoList) {
			String v = "( '" + varInfo.getTime() + "','"
							 + varInfo.getOffset() + "','"
							 + varInfo.getLength() + "','"
							 + varInfo.getBlockHosts() + "','"
							 + varInfo.getComp_Code()
					   + "')";
			values = values + v + ",";
		}
		
		String time = varInfoList.get(varInfoList.size()-1).getTime() + "";
		
		values = values.substring(0, values.length()-1) + ";";
		
		String sql;
		if(dateType.equals("Daily")/*varInfo.getTime()>100000000*/) {
			sql = "insert into \"" + varShortName+"_Daily" + "\"(\"Time\", \"Offset\", \"Length\", \"BlockHost\", \"Comp_Code\") values" 
			  		+ values;
		} else {
			sql = "insert into \"" + varShortName+"_Monthly" + "\"(\"Time\", \"Offset\", \"Length\", \"BlockHost\", \"Comp_Code\") values" 
			  		+ values;
		}	
		
		LOG.info(sql);
		
		try {
			this.statement.execute(sql);
			//System.out.printf("Add %d pieces of records into Var = %s  time is %s  \n", varInfoList.size(), varShortName, time);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public void addVarRecord(VariableInfo varInfo, String dateType) {
		String sql;
		if(dateType.equals("Daily")/*varInfo.getTime()>100000000*/) {
			sql = "insert into \"" + varInfo.getShortName()+"_Daily" + "\"(\"Time\", \"Offset\", \"Length\", \"BlockHost\", \"Comp_Code\") values(" 
			  		+ "'" 
					+ varInfo.getTime()
					+ "','"
					+ varInfo.getOffset()
					+ "','"
					+ varInfo.getLength()
					+ "','"
					+ varInfo.getBlockHosts()
					+ "','"
					+ varInfo.getComp_Code()
					+ "')";
		} else {
			sql = "insert into \"" + varInfo.getShortName()+"_Monthly" + "\"(\"Time\", \"Offset\", \"Length\", \"BlockHost\", \"Comp_Code\") values(" 
			  		+ "'" 
					+ varInfo.getTime()
					+ "','"
					+ varInfo.getOffset()
					+ "','"
					+ varInfo.getLength()
					+ "','"
					+ varInfo.getBlockHosts()
					+ "','"
					+ varInfo.getComp_Code()
					+ "')";
		}	
		
		try {
			this.statement.execute(sql);
			System.out.printf("Add record Var = %s Time = %d \n", varInfo.getShortName(), varInfo.getTime());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void deleteRecords(String startTime, String endTime, String varShortName) {
		String sql;
		if(Integer.parseInt(startTime)>100000000) {
			sql = "delete FROM MerraIndex." + varShortName + "_Daily" + " where \"Time\">=" + startTime + " and \"Time\"<=" + endTime;
		} else {
			sql = "delete FROM MerraIndex." + varShortName + "_Monthly" + " where \"Time\">=" + startTime + " and \"Time\"<=" + endTime;
		}
		
		try {
			this.statement.execute(sql);
			System.out.printf("Delete records from %s to %s in table %s \n", startTime, endTime, varShortName);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public List<VariableInfo> getVarInfoByTimeQuery(int start, int end, String varName) {
		String sql;
		List<VariableInfo> varInfoList = new ArrayList<VariableInfo>();
		
		if(start > 100000000) {
			//be careful about the time format
			sql = "SELECT * FROM \"" + varName + "_Daily\" WHERE \"Time\">=" + start + " and \"Time\"<=" + end;
			ResultSet rs;
			
			try {
				rs = statement.executeQuery(sql);				
				varInfoList.addAll(constructVarInfoList(rs, varName));	
				rs.close();
				//System.out.println("get rs rows =======" + varInfoList.size() + "-----------" + varName);
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}						
		} else {
			sql = "SELECT * FROM \"" + varName + "_Monthly\" WHERE \"Time\">=" + start + " and \"Time\"<=" + end;
			ResultSet rs;
			
			try {
				rs = statement.executeQuery(sql);
				varInfoList.addAll(constructVarInfoList(rs, varName));	
				rs.close();
				//System.out.println("get rs rows =======" + varInfoList.size() + "-----------" + varName);
				
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
		
		return varInfoList;
	}
	
	public List<VariableInfo> getVarInfoByTimeQuery(int start, int end, String varName, String dateType) {
		String sql;
		List<VariableInfo> varInfoList = new ArrayList<VariableInfo>();
		
		if(dateType.equals("Daily")) {
			//be careful about the time format
			sql = "SELECT * FROM \"" + varName + "_Daily\" WHERE \"Time\">=" + start + " and \"Time\"<=" + end;
			ResultSet rs;
			
			try {
				rs = statement.executeQuery(sql);				
				varInfoList.addAll(constructVarInfoList(rs, varName));	
				rs.close();
				//System.out.println("get rs rows =======" + varInfoList.size() + "-----------" + varName);
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}						
		} else {
			sql = "SELECT * FROM \"" + varName + "_Monthly\" WHERE \"Time\">=" + start + " and \"Time\"<=" + end;
			ResultSet rs;
			
			try {
				rs = statement.executeQuery(sql);
				varInfoList.addAll(constructVarInfoList(rs, varName));	
				rs.close();
				//System.out.println("get rs rows =======" + varInfoList.size() + "-----------" + varName);
				
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
		
		return varInfoList;
	}

	
	public List<VariableInfo> constructVarInfoList(ResultSet rs, String varName) {
		List<VariableInfo> varInfoList = new ArrayList<VariableInfo>();
		try {
			rs.last();
			if (rs.getRow() == 0) {
				System.out.println("###DBOperator.constructVarInfo: ResultSet is empty");
				rs.close();
			} else {
				rs.first();
				do 
				{				
					int time = rs.getInt("Time");
					long offset = rs.getLong("Offset");
					long length = rs.getLong("Length");
					String blockHosts = rs.getString("BlockHost");
					int comp_Code = rs.getInt("Comp_Code");
					VariableInfo varInfo = new VariableInfo(varName, time, offset, length, blockHosts, comp_Code);
					
					varInfoList.add(varInfo);

					rs.next();
				} while (!rs.isAfterLast());
			}
		} catch (Exception ex) {
			System.out.println(ex.toString());
		}
		return varInfoList;
	}
	
	public static void main(String[] args) {
		DBOperator dbOperator = new DBOperator(new DBConnector().GetConnStatement());
		//dbOperator.createTable("GRN");
		//VariableInfo var = new VariableInfo("GRN", 20140201, 123, 2345, "adfsdf", 4);
		//dbOperator.addVarRecord(var);
	}

}
