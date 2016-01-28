package edu.gmu.stc.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import edu.gmu.stc.configure.MyProperty;


/**
 * @author feihu
 *
 */
public class DBConnector {
	// private Connection connection = null;
	// ","root","eie%cisc"
	/**
	 * MySQL basic connection string. Change localhost to your IP address and
	 * wmsportal to your database if necessary.
	 */
	public String connString = MyProperty.mysql_connString;//"jdbc:mysql://199.26.254.190:3306/climateviz";
	//public String connString = "jdbc:mysql://localhost:3306/climateviz"; //server
	
	/**
	 * MySQL login name, default login name is root
	 */
	public String user = MyProperty.mysql_user;//"climatevizuser";
	/**
	 * MySQL login password
	 */
	public String password = MyProperty.mysql_password;//"climateviz";
	private String catalog = MyProperty.mysql_catalog;//"climateviz";
	private String dbDriver = MyProperty.dbDriver;
	
	public Connection connection = null;

	Statement statement = null;

	/**
	 * Constructor, passing the the necessary params to initate a MySQL
	 * connection
	 * 
	 * @param connString
	 *            basic connection string
	 * @param user
	 *            MySQL login name
	 * @param password
	 *            MySQL login password
	 */
	public DBConnector(String connString, String user, String password, String catalog) {
		super();
		this.connString = connString;
		this.user = user;
		this.password = password;
		this.catalog = catalog;
		this.connection = this.Connect(connString, user, password, catalog);

	}

	public DBConnector() {
		super();
		System.out.println("******** Start to connect PostGreSQL");
		this.connection = this.Connect(this.connString, this.user,
				this.password, this.catalog);

		try {
			System.out.println("connected" + this.connString + this.user + this.password + this.catalog);
			this.statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE,
			         ResultSet.HOLD_CURSORS_OVER_COMMIT);
			System.out.println("statement created");
		} catch (Exception e) {
			System.out.println("Cannot createStatement===");
			System.out.print(e);
			// System.exit(0);
		}

	}

	/**
	 * Make sure to call this function whenever you have finished an Database
	 * operation
	 */
	public void CloseConnection() {

		try {
			this.connection.close();
		} catch (Exception e) {
			System.out.print(e);
			// System.exit(0);
		}
	}

	/**
	 * Connect to a MySQL database based on the provided params.
	 * 
	 * @param connUrl
	 *            basic connection string
	 * @param user
	 *            MySQL login name
	 * @param password
	 *            MySQL login password
	 * @return A connection session
	 * @see Connection
	 */
	public Connection Connect(String connUrl, String user, String password, String catalog) {
		//try {
			try {
				Class.forName(this.dbDriver).newInstance ();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Connection connection;
			try {
				connection = DriverManager.getConnection(connUrl, user,
						password);
				connection.setCatalog(catalog);
				return connection;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
			
		//} catch (Exception ex) {
			//System.err.println ("Cannot connect to database server");
			//return null;
		//}
	}
	
	public Connection Connect(String dbDriver, String connUrl, String user, String password, String catalog) {
		//try {
			try {
				Class.forName(dbDriver).newInstance ();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Connection connection;
			try {
				connection = DriverManager.getConnection(connUrl, user,
						password);
				connection.setCatalog(catalog);
				return connection;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
			
		//} catch (Exception ex) {
			//System.err.println ("Cannot connect to database server");
			//return null;
		//}
	}
	
	

	/**
	 * Get the connection statement
	 * 
	 * @return the connection statement
	 */
	public Statement GetConnStatement() {
		
		return this.statement;
	}


	/**
	 * Get the connection statement
	 * 
	 * @return the connection statement
	 */
	public Connection GetConnection() {

		return this.connection;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		double starttime= System.currentTimeMillis();
		DBConnector dbconn = new DBConnector();
		double endtime= System.currentTimeMillis();
		System.out.println(endtime-starttime);
		//dbconn.updateLatLon();
		dbconn.CloseConnection();

	}
}
