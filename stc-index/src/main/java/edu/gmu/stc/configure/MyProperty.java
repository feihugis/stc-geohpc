package edu.gmu.stc.configure;


public class MyProperty {
	
	public static String  HADOOP_HOST_NAME ="10.10.3.200";//"199.26.254.154";
	public static String  HADOOP_ADMIN_USER= "root";
	public static String  HADOOP_PASSWORD= "1q2w3e4r";//"wanghuifen123?"

	public static final  String serviceUrlBase="http://10.10.3.200/output/";//"http://199.26.254.154/output/";
	public static final String hadoopHome = "/etc/hadoop/conf.cloudera.yarn";// "/home/hadoop-lzl/hadoop-1.0.4";
	
	public static final  String serverDirBase = "/var/www/html/output/"; //make sure namenode has this folder
	public static final String fetchedDataLocationBaseDir = "/dataFetched/";//"/home/zhenlong/";//
	public static final String hdfsOutputBaseDir="/workflowOutput/";
	
	public static String dbDriver = "org.postgresql.Driver"; //"com.mysql.jdbc.Driver"
	
	//For NCCS
	/*public static String mysql_connString =  "jdbc:postgresql://10.0.0.147:5432/MerraIndex";//"jdbc:postgresql://199.26.254.149:5432/MerraIndex";//"jdbc:mysql://199.26.254.190:3306/MerraIndex";
	public static String mysql_user = "postgres"; //"root"; 
	public static String mysql_password = "netcdf"; //cisc255b"; 
	public static String mysql_catalog = "MerraIndex";
	public static String nameNode = "hdfs://nameservice1";*/
	
	//For CISC
	/*public static String mysql_connString = "jdbc:postgresql://199.26.254.149:5432/MerraIndex";//"jdbc:mysql://199.26.254.190:3306/MerraIndex"; 
	public static String mysql_user = "postgres"; //"root"; 
	public static String mysql_password = "wanghuifen123?"; 
	public static String mysql_catalog = "MerraIndex";
	public static String nameNode = "hdfs://199.26.254.154:9000";*/

    //For data center
	/* Index for Merra
	public static String mysql_connString = "jdbc:postgresql://192.168.8.84:5432/MerraIndex";//"jdbc:mysql://199.26.254.190:3306/MerraIndex";
	public static String mysql_user = "postgres"; //"root";
	public static String mysql_password = "cisc255b";
	public static String mysql_catalog = "MerraIndex";
	public static String nameNode = "hdfs://SERVER-A8-C-U26:8020";*/

        /*public static String mysql_connString = "jdbc:postgresql://192.168.8.84:5432/merra2";//"jdbc:mysql://199.26.254.190:3306/MerraIndex";
	public static String mysql_user = "postgres"; //"root";
	public static String mysql_password = "cisc255b";
	public static String mysql_catalog = "MerraIndex";
	public static String nameNode = "hdfs://SERVER-A8-C-U26:8020";*/
      //data container
      //Index for Merra2
	/*public static String mysql_connString = "jdbc:postgresql://192.168.2.253:5432/merra2datacontainer";//"jdbc:mysql://199.26.254.190:3306/MerraIndex";10.192.21.253
	public static String mysql_user = "postgres"; //"root";
	public static String mysql_password = "cisc255b";
	public static String mysql_catalog = "merra2datacontainer";
	public static String nameNode = "hdfs://svr-A3-A-U2:8020";
	public static String geoJSONPath = "/gz_2010_us_040_00_500k.json";
        public static String gifOutputPath = "/var/lib/hadoop-hdfs/gif/";*/

        public static String ClimateHadoop_Config_FilePath = "/stc-geohpc/stc-spark/src/main/resources/mod08-climatespark-config.xml";

        //data container svr-A3-A-U17
        //Index for Merra2
        public static String mysql_connString = "jdbc:postgresql://10.3.1.16:5432/merra2datacontainer";//"jdbc:mysql://199.26.254.190:3306/MerraIndex";
        public static String mysql_user = "feihu"; //"root";
        public static String mysql_password = "cisc255b";
        public static String mysql_catalog = "merra2datacontainer";
        public static String nameNode = "hdfs://svr-A3-A-U2:8020";
        public static String geoJSONPath = "/gz_2010_us_040_00_500k.json";
        public static String gifOutputPath = "/var/lib/hadoop-hdfs/gif/";

        public static final String TABLE_PREFIX = "merra2_100_tavg1_2d_int_nx_";
        public static final String TABLE_POSTFIX = "_nc4";
        public static final String HDFS_FilePATH_PREFIX = "/merra2/daily/M2T1NXINT/"; //"/Users/feihu/Documents/Data/M2T1NXINT/"; //"/merra2/daily/M2T1NXINT/";
        public static final String MERRA2_FILE_PREFIX = "MERRA2_100.tavg1_2d_int_Nx.";
        public static final String MERRA2_FILE_PREFIX_Sec = "MERRA2_200.tavg1_2d_int_Nx.";
        public static final String MERRA2_FILE_POSTFIX = ".nc4";
        public static final String PRODUCT_NAME = "M2T1NXINT";

        //For localhost
        /*public static String db_host = "jdbc:postgresql://localhost:5432/";
        public static String mysql_connString = "jdbc:postgresql://localhost:5432/mod08"; //merra2testv2";//"jdbc:mysql://199.26.254.190:3306/MerraIndex";
        public static String mysql_user = "feihu"; //"root";
        public static String mysql_password = "feihu";
        public static String mysql_catalog = "mod08"; //"merra2testv2";
        public static String nameNode = "file:////";
        public static String geoJSONPath = "/Users/feihu/Desktop/gz_2010_us_040_00_500k.json";

        public static final String TABLE_PREFIX = "merra2_100_tavg1_2d_int_nx_";
        public static final String TABLE_POSTFIX = "_nc4";
        public static final String HDFS_FilePATH_PREFIX = "/Users/feihu/Documents/Data/M2T1NXINT/"; //"/merra2/daily/M2T1NXINT/";
        public static final String MERRA2_FILE_PREFIX = "MERRA2_100.tavg1_2d_int_Nx.";
        public static final String MERRA2_FILE_PREFIX_Sec = "MERRA2_200.tavg1_2d_int_Nx.";
        public static final String MERRA2_FILE_POSTFIX = ".nc4";
        public static final String PRODUCT_NAME = "M2T1NXINT";*/



	public MyProperty() {
		// TODO Auto-generated constructor stub
	}
}
