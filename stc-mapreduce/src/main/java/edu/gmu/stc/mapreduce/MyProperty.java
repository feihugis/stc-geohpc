package edu.gmu.stc.mapreduce;


public class MyProperty {
	
	public static String  HADOOP_HOST_NAME ="10.10.3.200";//"199.26.254.154";
	public static String  HADOOP_ADMIN_USER= "root";
	public static String  HADOOP_PASSWORD= "1q2w3e4r";//"wanghuifen123?"

	public static final  String serviceUrlBase="http://10.10.3.200/output/";//"http://199.26.254.154/output/";
	public static final String hadoopHome = "/home/hadoop/hadoop/hadoop-1.0.4";// "/home/hadoop-lzl/hadoop-1.0.4";
	
	public static final  String serverDirBase = "/var/www/html/output/"; //make sure namenode has this folder
	public static final String fetchedDataLocationBaseDir = "/dataFetched/";//"/home/zhenlong/";//
	public static final String hdfsOutputBaseDir="/workflowOutput/";
	
	public static String dbDriver = "org.postgresql.Driver"; //"com.mysql.jdbc.Driver"
	
	//For NCCS
	public static String mysql_connString =  "jdbc:postgresql://10.0.0.147:5432/MerraIndex";//"jdbc:postgresql://199.26.254.149:5432/MerraIndex";//"jdbc:mysql://199.26.254.190:3306/MerraIndex"; 
	public static String mysql_user = "postgres"; //"root"; 
	public static String mysql_password = "netcdf"; //cisc255b"; 
	public static String mysql_catalog = "MerraIndex";
	public static String nameNode = "hdfs://nameservice1";
	
	//For CISC
	/*public static String mysql_connString = "jdbc:postgresql://199.26.254.149:5432/MerraIndex";//"jdbc:mysql://199.26.254.190:3306/MerraIndex"; 
	public static String mysql_user = "postgres"; //"root"; 
	public static String mysql_password = "wanghuifen123?"; 
	public static String mysql_catalog = "MerraIndex";
	public static String nameNode = "hdfs://199.26.254.154:9000";*/


	public MyProperty() {
		// TODO Auto-generated constructor stub
	}
}
