package edu.gmu.stc.database;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.configure.MyProperty;
import edu.gmu.stc.hadoop.index.io.merra.NcHdfsRaf;
import edu.gmu.stc.hadoop.index.io.merra.VariableInfo;
import edu.gmu.stc.hadoop.index.tools.BuildIndex;
import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;


/**
 * @author feihu
 *
 */
public class IndexOperator {
	
	private FileSystem fs = null;
	private FSDataInputStream fsInput = null;
	private Configuration conf = new Configuration();
	private DBOperator dbOperator = null;	
	
	public IndexOperator() {
		conf.set("fs.defaultFS", MyProperty.nameNode);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
		dbOperator = new DBOperator(new DBConnector().GetConnStatement());
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void createTable(String tableName) {
		dbOperator.createTable(tableName);
	}
	
	public void deleteAllTable(String schema) {
		dbOperator.deleteAllTables(schema);
	}
	
	public void testTileStructure() {
		try {
			NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(new Path("/MerraData/Daily/MERRA300.prod.assim.tavg1_2d_int_Nx.20150101.hdf")), fs.getConf());
			NetcdfFile ncfile = NetcdfFile.open(raf, "/MerraData/Daily/MERRA300.prod.assim.tavg1_2d_int_Nx.20150101.hdf");
			
			int[] start = new int[3];
			start[0] = 0;start[1] = 0;start[2] = 0;
			int[] shape = new int[3];
			shape[0] = 1;shape[1] = 1;shape[2] = 1;
			
			long time = System.currentTimeMillis();
			Variable yDim = ncfile.findVariable("EOSGRID/Data_Fields/YDim");		
			Array aa =  yDim.read();
			System.out.println("aa   " + aa.getIndexIterator().getDoubleNext());
			
			long time1 = System.currentTimeMillis();
			
			
			Variable v = ncfile.findVariable("EOSGRID/Data_Fields/SUBCI");
			Array bb = v.read(start, shape);
			System.out.println(bb.getSize() + "bb   " + bb.getIndexIterator().getFloatNext());
			
			long time2 = System.currentTimeMillis();
			
			long gap = (time2 - time1) - (time1 - time);
			System.out.println("Gap time " + gap + " , " + (time2 - time1) + "," + (time1 - time));
			
			
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidRangeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}
	
	public void createTables(String sampleFilePath) {
		try {		
			//NetcdfFile ncfile = NetcdfFile.open(sampleFilePath);
			NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(new Path(sampleFilePath)), fs.getConf());
			NetcdfFile ncfile = NetcdfFile.open(raf, sampleFilePath);
			List<Variable> varList = ncfile.getVariables();
			for(Variable var : varList) {
				if(var.getFullName().contains("EOSGRID/Data_Fields/") && !var.isCoordinateVariable() && !var.isMetadata()) {
					dbOperator.createTable(var.getShortName() + "_Daily");
					dbOperator.createTable(var.getShortName() + "_Monthly");
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void deleteAllRecords(String sampleFilePath, String startTime, String endTime) {
		try {
			//NetcdfFile ncfile = NetcdfFile.open(sampleFilePath);
			NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(new Path(sampleFilePath)), fs.getConf());
			NetcdfFile ncfile = NetcdfFile.open(raf, sampleFilePath);
			List<Variable> varList = ncfile.getVariables();
			for(Variable var : varList) {
				if(var.getFullName().contains("EOSGRID/Data_Fields/") && !var.isCoordinateVariable() && !var.isMetadata()) {
					dbOperator.deleteRecords(startTime, endTime, var.getShortName());
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
		
	public void addIndexRecords(Path path, int sDate, int eDate) {
		try {
			//FileStatus[] fileStatusList = fs.listStatus(path);
			List<FileStatus> fileStatusList = new ArrayList<FileStatus>();
			RemoteIterator<LocatedFileStatus> ritr = fs.listFiles(path, true);
			while(ritr.hasNext()) {
				fileStatusList.add(ritr.next());
			}
			
			//Filter the files that is not HDF format
			ArrayList<FileStatus> unHDFs = new ArrayList<FileStatus>();
		    for(FileStatus file: fileStatusList) {
		    	String location = file.getPath().toString();
		    	String[] paths = location.split("\\.");
		    	String format = paths[paths.length-1];
		    	if(!format.equalsIgnoreCase("hdf")) {
		    		unHDFs.add(file);
		    	}	
		    }    
		    
		    fileStatusList.removeAll(unHDFs);
			
			for(FileStatus fileStatus : fileStatusList) {
				String[] file = fileStatus.getPath().toString().split("\\.");
				String date = file[file.length-2];
				
				int dd = Integer.parseInt(date);
				if(sDate>dd || eDate<dd) {
					continue;
				}
				
				NcHdfsRaf raf = new NcHdfsRaf(fileStatus, conf);
				NetcdfFile ncfile = NetcdfFile.open(raf, fileStatus.getPath().toString());
				List<Variable> varList = new ArrayList<Variable>();
				varList = ncfile.getVariables();
				
				
				for(Variable var : varList) {
					if(!var.getFullName().contains("EOSGRID/Data_Fields/") || var.isCoordinateVariable() && var.isMetadata() || var.getFullName().equals("Time")
							|| var.getShortName().equals("XDim")|| var.getShortName().equals("YDim")
							|| var.getShortName().equals("Height")|| var.getShortName().equals("Time")) {
						continue;
					}
					
					int hour = 0; //For daily data, "hour" means hour, but for monthly data, "hour" means day
					String varInfo = var.getVarLocationInformation();
					String[] unitsV = varInfo.split("; ");
					String dateType = "";
					List<VariableInfo> varInfoList = new ArrayList<VariableInfo>();
					for(int i=0; i<(unitsV.length); i++) {
						String unit = unitsV[i];
						String[] p1 = unit.split(", ")[0].split(" ");
						String[] p2 = unit.split(", ")[1].split(" ");
						String d = p1[p1.length-1];
						if(d.equals("data")) {
							System.out.println(varInfo);
						}
						long offset = Long.parseLong(p1[p1.length-1]);
						long len = Long.parseLong(p2[p2.length-1]);
						BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, offset, len);
						String blockHosts = new String();
						for(BlockLocation blockLoc : blockLocations) {
							String[] names = blockLoc.getNames();
							String[] hosts = blockLoc.getHosts();
							//String blockString = "Blocks: " + combineStringArray(names, " ") + "__";
							String hostString = "Hosts: " + combineStringArray(hosts, " ") + "; ";
							blockHosts = hostString;
							//blockHosts = blockString + hostString;
						}
						
						//String time;
						String dims_noSpace = date + getDimension(p1[0]);
						/*if(hour<10) {
							time = date + "0" + hour;
						} else {
							time = date + hour;
						}*/
						
						
						if(dd<1000000) {
							dateType = "Monthly";
						} else {
							dateType = "Daily";
						}
						VariableInfo unitVInfo = new VariableInfo(var.getShortName(), Integer.parseInt(dims_noSpace), offset, len, blockHosts, 4);
						varInfoList.add(unitVInfo);
						//dbOperator.addVarRecord(unitVInfo, dateType);
						//hour++;
					}	
					
					dbOperator.addVarRecordList(varInfoList, dateType);
					
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidRangeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**get the startbyte and endbyte location information of the variables referred by the input parameters
	 * @param file
	 * @param varNames
	 * @return
	 */
	public List<VariableInfo> getVariableInfo(String file, String[] varNames) {
		List<VariableInfo> varInfoList = new ArrayList<VariableInfo>();
		String dateType = "";
		
		try {
			FileStatus fStatus = fs.getFileStatus(new Path(file));
			String[] tmp = file.split("\\.");
			String date = tmp[tmp.length-2];		
			int dd = Integer.parseInt(date);
			
			NcHdfsRaf raf = new NcHdfsRaf(fStatus, conf);
			NetcdfFile ncfile = NetcdfFile.open(raf, fStatus.getPath().toString());
			List<Variable> varList = new ArrayList<Variable>();
			varList = ncfile.getVariables();
			
			for(Variable var : varList) {
				for(String varName : varNames) {
					if(var.getShortName().equals(varName) || var.getFullName().equals(varName)) {
						String varInfo = var.getVarLocationInformation();
						String[] unitsV = varInfo.split("; ");
		
						for(int i=0; i<(unitsV.length); i++) {
							String unit = unitsV[i];
							String[] p1 = unit.split(", ")[0].split(" ");
							String[] p2 = unit.split(", ")[1].split(" ");
							String d = p1[p1.length-1];
							if(d.equals("data")) {
								System.out.println(varInfo);
							}
							long offset = Long.parseLong(p1[p1.length-1]);
							long len = Long.parseLong(p2[p2.length-1]);
							BlockLocation[] blockLocations = fs.getFileBlockLocations(fStatus, offset, len);
							String blockHosts = new String();
							for(BlockLocation blockLoc : blockLocations) {
								String[] names = blockLoc.getNames();
								String[] hosts = blockLoc.getHosts();
								//String blockString = "Blocks: " + combineStringArray(names, " ") + "__";
								String hostString = "Hosts: " + combineStringArray(hosts, " ") + "; ";
								blockHosts = hostString;
								//blockHosts = blockString + hostString;
							}
							
							//dimension information except latitude and longitude
							String dims_noSpace = date + getDimension(p1[0]);
							
							//judge whether the data is monthly or daily
							if(dd<1000000) {
								dateType = "Monthly";
							} else {
								dateType = "Daily";
							}
							VariableInfo unitVInfo = new VariableInfo(var.getShortName(), Integer.parseInt(dims_noSpace), offset, len, blockHosts, 4);
							unitVInfo.setDateType(dateType);
							varInfoList.add(unitVInfo);
							}
						}
					}  
				}
			}catch (IllegalArgumentException e) {
					    e.printStackTrace();
			} catch (IOException e) {
					    e.printStackTrace();
			} catch (InvalidRangeException e) {
						e.printStackTrace();
			}	
		
		return varInfoList;
	}
	
	public void addIndexRecordsByVarName(String[] files, String varName) {
		List<VariableInfo> varInfoList = new ArrayList<VariableInfo>();
		String dateType = "";
		
		for(String file : files) {
			try {

				FileStatus fStatus = fs.getFileStatus(new Path(file));
				String[] tmp = file.split("\\.");
				String date = tmp[tmp.length-2];

				int dd = Integer.parseInt(date);

				NcHdfsRaf raf = new NcHdfsRaf(fStatus, conf);
				NetcdfFile ncfile = NetcdfFile.open(raf, fStatus.getPath().toString());
				List<Variable> varList = new ArrayList<Variable>();
				varList = ncfile.getVariables();

				for(Variable var : varList) {

					if(var.getShortName().equals(varName) || var.getFullName().equals(varName)) {
						int hour = 0; //For daily data, "hour" means hour, but for monthly data, "hour" means day
						String varInfo = var.getVarLocationInformation();
						String[] unitsV = varInfo.split("; ");

						for(int i=0; i<(unitsV.length); i++) {
							String unit = unitsV[i];
							String[] p1 = unit.split(", ")[0].split(" ");
							String[] p2 = unit.split(", ")[1].split(" ");
							String d = p1[p1.length-1];
							if(d.equals("data")) {
								System.out.println(varInfo);
							}
							long offset = Long.parseLong(p1[p1.length-1]);
							long len = Long.parseLong(p2[p2.length-1]);
							BlockLocation[] blockLocations = fs.getFileBlockLocations(fStatus, offset, len);
							String blockHosts = new String();
							for(BlockLocation blockLoc : blockLocations) {
								String[] names = blockLoc.getNames();
								String[] hosts = blockLoc.getHosts();
								//String blockString = "Blocks: " + combineStringArray(names, " ") + "__";
								String hostString = "Hosts: " + combineStringArray(hosts, " ") + "; ";
								blockHosts = hostString;
								//blockHosts = blockString + hostString;
							}

							//String time;
							String dims_noSpace = date + getDimension(p1[0]);

							if(dd<1000000) {
								dateType = "Monthly";
							} else {
								dateType = "Daily";
							}
							VariableInfo unitVInfo = new VariableInfo(var.getShortName(), Integer.parseInt(dims_noSpace), offset, len, blockHosts, 4);
							varInfoList.add(unitVInfo);
							}
						}
					}
				}catch (IllegalArgumentException e) {
					      e.printStackTrace();
				} catch (IOException e) {
					      e.printStackTrace();
				} catch (InvalidRangeException e) {
						  e.printStackTrace();
				}
			}
		dbOperator.addVarRecordList(varInfoList, dateType);
	}
	
	public void addVarRecordList(List<VariableInfo> varInfoList, String dateType) {
		this.dbOperator.addVarRecordList(varInfoList, dateType);
	}
	
	public void addIndexRecordsByVarName(Path path, int sDate, int eDate, String varName) {
		try {
			
			//FileStatus[] fileStatusList = fs.listStatus(path);
			List<FileStatus> fileStatusList = new ArrayList<FileStatus>();
			RemoteIterator<LocatedFileStatus> ritr = fs.listFiles(path, true);
			while(ritr.hasNext()) {
				fileStatusList.add(ritr.next());
			}
			
			//Filter the files that is not HDF format
			ArrayList<FileStatus> unHDFs = new ArrayList<FileStatus>();
		    for(FileStatus file: fileStatusList) {
		    	String location = file.getPath().toString();
		    	String[] paths = location.split("\\.");
		    	String format = paths[paths.length-1];
		    	if(!format.equalsIgnoreCase("hdf")) {
		    		unHDFs.add(file);
		    	}	
		    }    
		    
		    fileStatusList.removeAll(unHDFs);
			
			for(FileStatus fileStatus : fileStatusList) {
				String[] file = fileStatus.getPath().toString().split("\\.");
				String date = file[file.length-2];
				
				int dd = Integer.parseInt(date);
				if(sDate>dd || eDate<dd) {
					continue;
				}
				
				NcHdfsRaf raf = new NcHdfsRaf(fileStatus, conf);
				NetcdfFile ncfile = NetcdfFile.open(raf, fileStatus.getPath().toString());
				List<Variable> varList = new ArrayList<Variable>();
				varList = ncfile.getVariables();
				
				
				for(Variable var : varList) {
					if(!var.getFullName().equals(varName)) {
						continue;
					}
					
					int hour = 0; //For daily data, "hour" means hour, but for monthly data, "hour" means day
					String varInfo = var.getVarLocationInformation();
					String[] unitsV = varInfo.split("; ");
					String dateType = "";
					List<VariableInfo> varInfoList = new ArrayList<VariableInfo>();
					for(int i=0; i<(unitsV.length); i++) {
						String unit = unitsV[i];
						String[] p1 = unit.split(", ")[0].split(" ");
						String[] p2 = unit.split(", ")[1].split(" ");
						String d = p1[p1.length-1];
						if(d.equals("data")) {
							System.out.println(varInfo);
						}
						long offset = Long.parseLong(p1[p1.length-1]);
						long len = Long.parseLong(p2[p2.length-1]);
						BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, offset, len);
						String blockHosts = new String();
						for(BlockLocation blockLoc : blockLocations) {
							String[] names = blockLoc.getNames();
							String[] hosts = blockLoc.getHosts();
							//String blockString = "Blocks: " + combineStringArray(names, " ") + "__";
							String hostString = "Hosts: " + combineStringArray(hosts, " ") + "; ";
							blockHosts = hostString;
							//blockHosts = blockString + hostString;
						}
						
						//String time;
						String dims_noSpace = date + getDimension(p1[0]);
											
						if(dd<1000000) {
							dateType = "Monthly";
						} else {
							dateType = "Daily";
						}
						VariableInfo unitVInfo = new VariableInfo(var.getShortName(), Integer.parseInt(dims_noSpace), offset, len, blockHosts, 4);
						varInfoList.add(unitVInfo);
						//dbOperator.addVarRecord(unitVInfo, dateType);
						//hour++;
					}	
					
					dbOperator.addVarRecordList(varInfoList, dateType);
				}
				
				System.out.println(fileStatus.getPath().toString());
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidRangeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void deleteRecords(String startTime, String endTime, String varShortName) {
		this.dbOperator.deleteRecords(startTime, endTime, varShortName);
	}
	
	public List<VariableInfo> getVarInfoByTimeQuery(int startDate, int endDate, int sHour, int eHour, String varName) {
		List<VariableInfo> varInfoList = new ArrayList<VariableInfo>();
		List<VariableInfo> tmpList = new ArrayList<VariableInfo>();
		
		//TODO: fix the following logic to improve the performance
		int digit = 100;
		if(sHour<100) {
			digit = 100;
		} else if(sHour>=100 && sHour<10000) {
			digit = 10000;
		}
		
		String dateType = "";
		if(startDate<1000000) {
			dateType = "Monthly";
		} else {
			dateType = "Daily";
		}
		tmpList = getVarInfoByTimeQuery(startDate*digit+sHour, endDate*digit+eHour, varName);
		
		// filter the varInfo who is out the hour range
		for(VariableInfo varInfo : tmpList) {
			int time = varInfo.getTime();
			int hour = time % 100;
			if(hour>=sHour && hour<=eHour) {
				varInfoList.add(varInfo);
			}
		}
		
		return varInfoList;
	}
	
	public List<VariableInfo> getVarInfoByTimeQuery(int startDate, int endDate, String sHour, String eHour, String varName) {
		List<VariableInfo> varInfoList = new ArrayList<VariableInfo>();
		List<VariableInfo> tmpList = new ArrayList<VariableInfo>();
		
		//TODO: fix the following logic to improve the performance	
		String dateType = "";
		if(startDate<1000000) {
			dateType = "Monthly";
		} else {
			dateType = "Daily";
		}
		tmpList = getVarInfoByTimeQuery(Integer.parseInt(startDate+sHour), Integer.parseInt(endDate+eHour), varName, dateType);
		
		// filter the varInfo who is out the hour range
		for(VariableInfo varInfo : tmpList) {
			int time = varInfo.getTime();
			int hour = time % ((int) Math.pow(10.0, sHour.length()+0.0));
			if(hour>=Integer.parseInt(sHour) && hour<=Integer.parseInt(eHour)) {
				varInfoList.add(varInfo);
			}
		}
		
		return varInfoList;
	}
	
	public List<VariableInfo> getVarInfoByTimeQuery(int start, int end, String varName) {
		List<VariableInfo> varInfoList = new ArrayList<VariableInfo>();
		varInfoList.addAll(dbOperator.getVarInfoByTimeQuery(start, end, varName));
		return varInfoList;
	}
	
	public List<VariableInfo> getVarInfoByTimeQuery(int start, int end, String varName, String dateType) {
		List<VariableInfo> varInfoList = new ArrayList<VariableInfo>();
		varInfoList.addAll(dbOperator.getVarInfoByTimeQuery(start, end, varName, dateType));
		return varInfoList;
	}
	
	public String combineStringArray(String[] arrays, String symbol ) {
		String result = new String();
		for(String value : arrays) {
			result = result + value + symbol;
		}		
		return result;
	}
	
	/**
	 * Extract the dimension information( but without space information)
	 * @param dims  in the format e.g., 1:1,2:2,0:360,0:539
	 * @return
	 */
	public String getDimension(String dims) {
		String[] dimList = dims.split(",");
		String[] dims_noSpace = new String[dimList.length-2];
		for(int i=0; i<dims_noSpace.length; i++) {
			dims_noSpace[i] = dimList[i].split(":")[0];
			if(Integer.parseInt(dims_noSpace[i])<10) {
				dims_noSpace[i] = "0" + dims_noSpace[i];
			}
		}
		
		String result = "";
		for(String value : dims_noSpace) {
			result = result + value;
		}
		
		return result;		
	}

	public static void main(String[] args) {
		
		IndexOperator indexOperator = new IndexOperator();
		
		if(args.length < 1) {
			System.out.println("Please input an operation");
		}
		
		String operationName = args[0];
		
		if(operationName.equalsIgnoreCase("DeleteAllTables")) {
			indexOperator.deleteAllTable("public");
		}
		
		if(operationName.equalsIgnoreCase("CreateTables")) {
			if(args.length !=2) {
				System.out.println("Please input <operationName> <SampleFilePath>, for example CreateTables /Users/feihu/Documents/Data/Merra1.5GB/MERRA300.prod.assim.tavg1_2d_int_Nx.20150101.hdf");
				return;
			}
			String sampleFilePath = args[1];
			indexOperator.createTables(sampleFilePath);
		}
		
		if(operationName.equals("AddRecords")) {
			if(args.length !=4) {
				System.out.println("Please input <operationName> <inputDirectory> <startTime> <endTime>, for example AddRecords /MerraData/Daily/ 20140101 20140131");
				return;
			}
			Path inputDirectory = new Path(args[1]); //"hdfs://199.26.254.154:9000/MerraData/Daily/"
			int sDate = Integer.parseInt(args[2]);
			int eDate = Integer.parseInt(args[3]);
			indexOperator.addIndexRecords(inputDirectory, sDate, eDate);
		}
		
		if(operationName.equals("AddRecordsInParallel")) {
			if(args.length !=7) {
				System.out.println("Please input <operationName> <input> <output> <variableNames> <filesPerMap> <startTime> <EndTime>, for example AddRecordsInParallel /MerraData/Daily/ /output/ BKGERR,CONVKE 3 20150101 20150131");
				return;
			}
			
			String[] argus = new String[args.length-1];
			for(int i=1; i<args.length; i++) {
				argus[i-1] = args[i];
			}
			
			try {
				BuildIndex.main(argus);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		if(operationName.equals("AddRecordsByVarName")) {
			if(args.length !=5) {
				System.out.println("Please input <operationName> <inputDirectory> <startTime> <endTime> <fullVarName>, for example AddRecordsByVarName /MerraData/Daily/ 20140101 20140131 EOSGRID/Data_Fields/PRECLS");
				return;
			}
			Path inputDirectory = new Path(args[1]); //"hdfs://199.26.254.154:9000/MerraData/Daily/"
			int sDate = Integer.parseInt(args[2]);
			int eDate = Integer.parseInt(args[3]);
			String varName = new String(args[4]);
			indexOperator.addIndexRecordsByVarName(inputDirectory, sDate, eDate, varName);
		}
		
		
		
		//indexOperator.createTables();
		//int sDate = Integer.parseInt(args[0]);
		//int eDate = Integer.parseInt(args[1]);
		//indexOperator.addIndexRecords(new Path("hdfs://199.26.254.154:9000/MerraData/Daily/"), sDate, eDate);
		//indexOperator.addIndexRecords(new Path("hdfs://199.26.254.154:9000/MerraData/Daily/"), 2015010101, 2015013123);
		//indexOperator.deleteRecords("2014010100", "2015010101", "LAI");
		//indexOperator.deleteAllRecords("/Users/feihu/Documents/Data/Merra100S/MERRA300.prod.simul.tavg1_2d_mld_Nx.20141001.hdf", "2014010100", "2015010101");
		//indexOperator.createTables("/Users/feihu/Documents/Data/Merra1.5GB/MERRA300.prod.assim.tavg1_2d_int_Nx.20150101.hdf");
		
		/*NetcdfFile ncfile;
		String vars = "";
		try {
			ncfile = NetcdfFile.open("/Users/feihu/Documents/Data/Merra1.5GB/MERRA300.prod.assim.tavg1_2d_int_Nx.20150101.hdf");
			List<Variable> varList = ncfile.getVariables();
			for(Variable var : varList) {
				if(var.getFullName().contains("EOSGRID/Data_Fields/") && !var.isCoordinateVariable() && !var.isMetadata()) {
					vars += var.getShortName()+",";
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(vars);*/
		
	}
	
}
