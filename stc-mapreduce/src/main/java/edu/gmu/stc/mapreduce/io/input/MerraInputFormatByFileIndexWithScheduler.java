package edu.gmu.stc.mapreduce.io.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import edu.gmu.stc.mapreduce.Utils;
import edu.gmu.stc.mapreduce.io.ArraySpec;
import edu.gmu.stc.mapreduce.io.NcHdfsRaf;
import edu.gmu.stc.mapreduce.io.SplitJson;
import edu.gmu.stc.mapreduce.io.VariableInfo;
import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import com.google.gson.Gson;



public class MerraInputFormatByFileIndexWithScheduler extends FileInputFormat {
	private static final Log LOG = LogFactory.getLog(MerraInputFormatByFileIndexWithScheduler.class);
	
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		  String indexDir = "./conf/index/";
		  String varDir = "./conf/index/vars/";
		  String varIndexFile = "./conf/index/vars/var.txt";
		  
		  List<FileStatus> files = listStatus(job);
		  LOG.info("Start hadoop*****************************************\t");
		  
		  List<String> variableQueryArray = getVariableQuearyList(job.getConfiguration().getStrings("variables"));

		  String[] vars = new String[variableQueryArray.size()];
		  
		  for(int i=0; i<variableQueryArray.size(); i++) {
			  vars[i] = variableQueryArray.get(i);
		  }
		  
		  List<FileStatus> filesWithoutIndex = new ArrayList<FileStatus>();
		  
		  //read index files to create SplitJson, and record the new files without index
		  List<SplitJson> jsnList = createSplitJsonByIndexFile(indexDir, files, filesWithoutIndex);
		  
		  if(filesWithoutIndex.size()>0) {
			  LOG.info("*******************************" +        "****" +            "******************************************************");
			  LOG.info("******************** There are " + filesWithoutIndex.size() + " new files needed to build indexes********************");
			  LOG.info("*******************************" +        "****" +            "******************************************************");
			  
			  buildIndexFiles(job, filesWithoutIndex, indexDir, varDir);
			  jsnList.addAll(createSplitJsonByIndexFile(indexDir, filesWithoutIndex));
		  }
		    
		  List<VariableInfo> varList = createVarInfoListByFile(varIndexFile, vars, files.get(0),job);
		  
		  String inputBbox = job.getConfiguration().get("bbox");
		  HashMap<String, List<Integer[]>> bboxMap = Utils.parseBbox(inputBbox);
		  
		  List<Integer[]> startList = new ArrayList<Integer[]>(bboxMap.get("start"));
		  List<Integer[]> endList = new ArrayList<Integer[]>(bboxMap.get("end"));
		  List<InputSplit> inputSplits = new ArrayList<InputSplit>();
		  List<CombineArrayBasedFileSplit> combineSplits = new ArrayList<CombineArrayBasedFileSplit>();
		  
		  
		  for(int i=0; i<startList.size(); i++) {
			   int[] start = new int[startList.get(i).length];
			   int[] end = new int[startList.get(i).length];
			   for(int j=0; j<startList.get(i).length; j++) {
				   start[j] = startList.get(i)[j];
				   end[j] = endList.get(i)[j];
			   }
			   
			   //Need to fix the bug that using i as mark of reducer partitioner
			   inputSplits = createInputSplits(jsnList, varList, start, end, i);		
			   int datanodeNum =  6 , slotNum = 4;
			   datanodeNum = Integer.parseInt(job.getConfiguration().get("datanodeNum"));
			   slotNum = Integer.parseInt(job.getConfiguration().get("slotNum"));
			   MerraFileInputScheduler scheduler = new MerraFileInputScheduler(inputSplits, datanodeNum, slotNum);
			   
			   combineSplits = MerraFileInputScheduler.combineInputSplit(combineSplits, scheduler.reOrganizeFileInput());		   
		  }
		  
		  List<InputSplit> finalSplitList = new ArrayList<InputSplit>(combineSplits);
		  
		  return finalSplitList;
	}
	
	/**
	 * @param variables  variable inputs from client
	 * @return List of variable names
	 */
	public List<String> getVariableQuearyList(String[] variables) {
		
		  List<String> variableQuearyList = new ArrayList<String>();
		 
		  for(int i=0; i<variables.length; i++) {
			  variableQuearyList.add(variables[i]);		  
		  }
		  
		  return variableQuearyList;
	  }
	
	/**
	 * create Split in JSON format for the input files and record those inputs without Index file
	 * 
	 * @param indexDir Index Directory
	 * @param inputfiles Input File to MapReduce
	 * @param filesWithoutIndex Records that do not have the corresponding index in the index directory
	 * @return Split in JSON format
	 */
	public List<SplitJson> createSplitJsonByIndexFile(String indexDir, List<FileStatus> inputfiles, List<FileStatus> filesWithoutIndex) {
		  List<SplitJson> splitJsnList = new ArrayList<SplitJson>();
		  Gson gson = new Gson();
		  for(FileStatus file : inputfiles) {
			  
		      try {
		    	  String path = file.getPath().toString();
			      int n = path.split("/").length;
			      String filename = path.split("/")[n-1]+".txt";
			      //String filePath = "./conf/index/" + filename;
			      String filePath = indexDir + filename;
			      
			      FileReader fr = new FileReader(filePath);
			      
			      BufferedReader br = new BufferedReader(fr);
			      String content = "", tmps;
			      while((tmps = br.readLine()) != null) {
			    	  content = content + tmps;
			      }
			      br.close();
			      fr.close();

			      SplitJson splitJsn = gson.fromJson(content, SplitJson.class);
				  splitJsnList.add(splitJsn);
			      
			      		      
			} catch (FileNotFoundException e) {
				//Judge whether the file index exists, if not, record it
				filesWithoutIndex.add(file);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  }  
		  
		  return splitJsnList;
	  }
	  
	
	/**
	 * create Split in JSON format for the input files 
	 * 
	 * @param indexDir Index Directory
	 * @param inputfiles Input File to MapReduce
	 * @return Split in JSON format
	 */
	  public List<SplitJson> createSplitJsonByIndexFile(String indexDir, List<FileStatus> inputfiles) {
		  List<SplitJson> splitJsnList = new ArrayList<SplitJson>();
		  Gson gson = new Gson();
		  for(FileStatus file : inputfiles) {
			  
		      try {
		    	  String path = file.getPath().toString();
			      int n = path.split("/").length;
			      String filename = path.split("/")[n-1]+".txt";
			      String filePath = indexDir + filename;
			      
			      FileReader fr = new FileReader(filePath);
			      BufferedReader br = new BufferedReader(fr);
			      String content = "", tmps;
			      while((tmps = br.readLine()) != null) {
			    	  content = content + tmps;
			      }
			      br.close();
			      fr.close(); 
	      
			      SplitJson splitJsn = gson.fromJson(content, SplitJson.class);
				  splitJsnList.add(splitJsn);
	 	      		      
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  }
		  
		  return splitJsnList;
	  }
	  
	  public void buildIndexFiles(JobContext job, List<FileStatus> filesForIndex, String indexDir, String varDir) throws IOException {
		    List<InputSplit> splits = new ArrayList<InputSplit>();
		    List<FileStatus> files = filesForIndex;
		   
		    List<Variable> variables = new ArrayList<Variable>();
		   
		    	    
		    for(FileStatus file: files) {
		    	try { 
		    	  LOG.info(file.getPath().toString());
		    	  
		  	      Path path = file.getPath();
		  	      FileSystem fs = path.getFileSystem(job.getConfiguration());
		  	      
		  	      if(variables.isEmpty()) {
		  	    	  NcHdfsRaf raf = new NcHdfsRaf( file, job.getConfiguration() );
			  	      NetcdfFile ncfile = NetcdfFile.open(raf, path.toString());	    
			  	      variables = ncfile.getVariables();			  	      
		  	      }
		  	      
		  	      BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, file.getLen());
		  	      long numBlocks = blocks.length;
		  	      //if( numBlocks >= 1 ) {
		  	       LOG.info("***********  File : " + path + " block size : " + numBlocks + "---" + blocks[0].toString());
		  	      //}
		  	      
		  	      
		  	      
		  	      List<String> hostsList = new ArrayList<String>();
		  	      
		  	      for(int i=0; i < numBlocks; i++) {
		  	    	  //System.arraycopy(blocks[i].getHosts(), 0, hosts, hosts.length, blocks[i].getHosts().length); 
		  	    	  
		  	    	  String[] blockhosts = blocks[i].getHosts();
		  	    	  for (int j = 0; j < blockhosts.length; j++) {
						hostsList.add(blockhosts[j]);
					}
		  	      }
		  	      
		  	      String[] hosts = new String[hostsList.size()];
		  	      hosts = hostsList.toArray(hosts);
		  	      ArrayBasedFileSplit split = new ArrayBasedFileSplit(path, hosts);
		  	      splits.add(split);
		  	       
		    	} catch (Exception e) {
		  	      System.out.println("Caught an exception in Building Index" + e.toString() );
		    	}
		    }
		    	
		    
		    createIndexFiles(splits, indexDir);    
		    createVarInfoFile(varDir, variables, "var");	    
	  }
	  
	  /**
	   * create index for the input files
	 * @param splits InputSplit
	 * @param indexDir Index directory
	 */
	public void createIndexFiles(List<InputSplit> splits, String indexDir) {
		  Gson gson = new Gson();
		  for(int i=0; i<splits.size(); i++) {	      
	  	      try {
	  	    	  ArrayBasedFileSplit split = (ArrayBasedFileSplit) splits.get(i);
	  	    	  SplitJson splitJson = new SplitJson(split.getPath(), split.getLocations());
	  	    	  String splitJsn = gson.toJson(splitJson);
	  	    	  
	  	    	  String path = split.getPath().toString();
	  	    	  int n = path.split("/").length;
	  	    	  String filename = path.split("/")[n-1];
	  	    	  
	  	    	  File file = new File(indexDir + filename + ".txt");
	  	    	  
	  	    	  if(!file.getParentFile().exists()) {
	  	    		  if( !file.getParentFile().mkdirs() ) {
	  	    			  LOG.info("Failed to create path directory");
	  	    		  }
	  	    		  
	  	    		  if( !file.createNewFile() ) {
	  	    			  LOG.info("Failed to create new index File");
	  	    		  }
	  	    	  }
	  	    	    	    	  
	  	    	  FileWriter writer = new FileWriter(file);
	  	    	  
	  	    	  writer.write(splitJsn);
	  	    	  writer.close();

		  		  } catch (IOException e) {
		  			 e.printStackTrace();
		  		  } catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	  		}
	  }
	  
	  /**
	 * @param indexDir Variable index directory
	 * @param varList  Variable List
	 * @param fileName the name of the variable index file
	 */
	public void createVarInfoFile(String indexDir, List<Variable> varList, String fileName){
		  VariableInfo[] varInfoList = new VariableInfo[varList.size()];
		  int i = 0;
		  for(Variable var : varList) {
			  
			    Float vmin=new Float(123456789.0), vmax=new Float(123456789.0),fillValue=new Float(123456789.0);
			    if(var.findAttribute("vmin") != null) {
			    	vmin = var.findAttribute("vmin").getNumericValue(0).floatValue();
			    }
			    if(var.findAttribute("vmax") != null) {
			    	vmax =  var.findAttribute("vmax").getNumericValue(0).floatValue();
			    }
			    if(var.findAttribute("_FillValue") != null) {
			    	fillValue =  var.findAttribute("_FillValue").getNumericValue(0).floatValue();
			    }
			  	
				VariableInfo varInfo = new VariableInfo(var.getShortName(), var.getFullName(), vmax.floatValue(), vmin.floatValue(), fillValue.floatValue());
				varInfoList[i] = varInfo;
				i++;
		  }
		  
		  Gson gson = new Gson();
		  
	  	  try {
	  		File file = new File(indexDir + fileName + ".txt");
	  		
	  		if(!file.getParentFile().exists()) {
	    		  if( !file.getParentFile().mkdirs() ) {
	    			  LOG.info("Failed to create path directory");
	    		  }
	    		  
	    		  if( !file.createNewFile() ) {
	    			  LOG.info("Failed to create new index File");
	    		  }
	    	  }
	  		
			FileWriter writer = new FileWriter(file);
			String vars = gson.toJson(varInfoList,VariableInfo[].class);
			writer.write(vars);
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }
	
	/**
	 * @param varDirFile File path name
	 * @param varList Variable List
	 */
	public void createVarInfoFile(String varDirFile, List<Variable> varList){
		  VariableInfo[] varInfoList = new VariableInfo[varList.size()];
		  int i = 0;
		  for(Variable var : varList) {
			    Float vmin=new Float(123456789.0), vmax=new Float(123456789.0),fillValue=new Float(123456789.0);
			    if(var.findAttribute("vmin") != null) {
			    	vmin = (Float) var.findAttribute("vmin").getValue(0);
			    }
			    if(var.findAttribute("vmax") != null) {
			    	vmax = (Float) var.findAttribute("vmax").getValue(0);
			    }
			    if(var.findAttribute("_FillValue") != null) {
				float tmp = var.findAttribute("_FillValue").getNumericValue().floatValue();
			    	fillValue = tmp; //(Float) var.findAttribute("_FillValue").getValue(0)*1.0f;
			    }

			    VariableInfo varInfo = new VariableInfo(var.getShortName(), var.getFullName(), vmax.floatValue(), vmin.floatValue(), fillValue.floatValue());
			    varInfoList[i] = varInfo;
			    i++;
		  }
		  
		  Gson gson = new Gson();
		  
	  	  try {
	  		File file = new File(varDirFile);
	  		
	  		if(!file.getParentFile().exists()) {
	    		  if( !file.getParentFile().mkdirs() ) {
	    			  LOG.info("Failed to create path directory");
	    		  }
	    		  
	    		  if( !file.createNewFile() ) {
	    			  LOG.info("Failed to create new index File");
	    		  }
	    	  }
	  		
			FileWriter writer = new FileWriter(file);
			String vars = gson.toJson(varInfoList,VariableInfo[].class);
			writer.write(vars);
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }
	
	/** According to the variable index file , create variable list. If the variable index file does not exist, create a file
	 * @param filePath File path name
	 * @param varNames Variable names
	 * @param file file in HDFS
	 * @param job Job context
	 * @return Variable Information
	 * @throws IOException
	 */
	public List<VariableInfo> createVarInfoListByFile(String filePath, String[] varNames, FileStatus file,JobContext job) throws IOException {
		  List<VariableInfo> varInfoList = new ArrayList<VariableInfo>();
		  List<VariableInfo> specVarInfoList = new ArrayList<VariableInfo>();
		  Gson gson = new Gson();
		   
		   try { 
			      FileReader fr = new FileReader(filePath);
			      BufferedReader br = new BufferedReader(fr);
			      String content = "", tmps;
			      while((tmps = br.readLine()) != null) {
			    	  content = content + tmps;
			      }
			      br.close();
			      fr.close();
			      
			      VariableInfo[] varInfos = gson.fromJson(content, VariableInfo[].class);
			      varInfoList = Arrays.asList(varInfos);
			      
			} catch (FileNotFoundException e) {
				  Path path = file.getPath();
				  NcHdfsRaf raf = new NcHdfsRaf( file, job.getConfiguration() );
				  NetcdfFile ncfile = NetcdfFile.open(raf, path.toString());
				  List<Variable> variables = ncfile.getVariables();
				  createVarInfoFile(filePath, variables);			  
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  
		  for(VariableInfo varInfo: varInfoList) {
			  for(int i=0; i<varNames.length; i++) {
				  if(varNames[i].equals(varInfo.getShortName())){
					  specVarInfoList.add(varInfo);
				  }
			  }
		  }
		  
		  return specVarInfoList;
	  }
	
	/**
	 * generate InputSplits for MapReduce
	 * @param splitList About the file path and hosts
	 * @param varInfos Variable Information
	 * @param startCorner Start point
	 * @param endCorner  End point
	 * @return InputSplit
	 */
	public List<InputSplit> createInputSplits(List<SplitJson> splitList, List<VariableInfo> varInfos, int[] startCorner, int[] endCorner) {
		  int time = endCorner[0] - startCorner[0];
		  int[] shape = new int[startCorner.length];
		  int[] corner = new int[startCorner.length];
		  int[] startOffset = new int[startCorner.length];
		  for(int i=0; i<corner.length; i++) {
			  corner[i] = startCorner[i];	
			  shape[i] = endCorner[i] - startCorner[i];
			  startOffset[i] = 0;
		  }
		  
		  shape[0] = 1;
		  
		  List<InputSplit> fileSplitsList = new ArrayList<InputSplit>();
		      
		  for(SplitJson splitJsn: splitList) {
			  ArrayList<ArraySpec> arraySpec = new ArrayList<ArraySpec>();
			  String uri = splitJsn.getPath().toString();
			  String[] tmp = uri.split("/");
			  String fileName = tmp[tmp.length-1];
			  
			  for(VariableInfo varInfo : varInfos) {
				  for(int i=0; i<time; i = i + shape[0]) {		  	
						try {
							corner[0] = startCorner[0] + i;
							ArraySpec tempSpec = new ArraySpec(corner, shape, varInfo.getFullName(), fileName);
							tempSpec.setLogicalStartOffset(startOffset);
							tempSpec.setVmax(varInfo.getVmax());
							tempSpec.setVmin(varInfo.getVmin());
							tempSpec.setFillValue(varInfo.getFillValue());
							arraySpec.add(tempSpec);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}	  
				  }
			  }
			  	 	  
			 ArrayBasedFileSplit split = new ArrayBasedFileSplit(splitJsn.getPath(), arraySpec, splitJsn.getHosts());
			 fileSplitsList.add(split);	  
		  }
		  
		  return fileSplitsList;
	  }
	
	/**
	 * generate InputSplits for MapReduce
	 * @param splitList About the file path and hosts
	 * @param varInfos Variable Information
	 * @param startCorner Start point
	 * @param endCorner  End point
	 * @param bboxType the type of bbox
	 * @return InputSplit
	 */
	public List<InputSplit> createInputSplits(List<SplitJson> splitList, List<VariableInfo> varInfos, int[] startCorner, int[] endCorner, int bboxType) {
		  int time = endCorner[0] - startCorner[0];
		  int[] shape = new int[startCorner.length];
		  int[] corner = new int[startCorner.length];
		  int[] startOffset = new int[startCorner.length];
		  for(int i=0; i<corner.length; i++) {
			  corner[i] = startCorner[i];	
			  shape[i] = endCorner[i] - startCorner[i];
			  startOffset[i] = 0;
		  }
		  
		  shape[0] = 1;
		  
		  List<InputSplit> fileSplitsList = new ArrayList<InputSplit>();
		      
		  for(SplitJson splitJsn: splitList) {
			  ArrayList<ArraySpec> arraySpec = new ArrayList<ArraySpec>();
			  String uri = splitJsn.getPath().toString();
			  String[] tmp = uri.split("/");
			  String fileName = tmp[tmp.length-1];
			  
			  for(VariableInfo varInfo : varInfos) {
				  for(int i=0; i<time; i = i + shape[0]) {		  	
						try {
							corner[0] = startCorner[0] + i;
							ArraySpec tempSpec = new ArraySpec(corner, shape, varInfo.getFullName(), fileName);
							tempSpec.setLogicalStartOffset(startOffset);
							tempSpec.setVmax(varInfo.getVmax());
							tempSpec.setVmin(varInfo.getVmin());
							tempSpec.setFillValue(varInfo.getFillValue());
							tempSpec.setBboxType(bboxType);
							arraySpec.add(tempSpec);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}	  
				  }
			  }
			  	 	  
			 ArrayBasedFileSplit split = new ArrayBasedFileSplit(splitJsn.getPath(), arraySpec, splitJsn.getHosts());
			 fileSplitsList.add(split);	  
		  }
		  
		  return fileSplitsList;
	  }
	 
	  /**
	   * Use FileInputFormat for file globbing capabilities, and filter out any
	   * files that are not in the time range and cannot be opened by NetcdfFile.open().
	   * @param jobC Context for the job being executed
	   * @return a List<FileStatus> where each entry represents a valid file
	   */
	  @Override
	  protected List<FileStatus> listStatus(JobContext jobC) throws IOException {
            //int startTime = Integer.parseInt(jobC.getConfiguration().get("startTime"));
	    //int endTime = Integer.parseInt(jobC.getConfiguration().get("endTime"));
	   long startTime = Long.parseLong(jobC.getConfiguration().get("startTime"));
            long endTime = Long.parseLong(jobC.getConfiguration().get("endTime"));

	    List<FileStatus> files = super.listStatus(jobC);
	    
	    ArrayList<FileStatus> nfiles = new ArrayList<FileStatus>();
	    for (FileStatus file: files) {
	    	String path = file.getPath().toString();

                //For modis data
                if (path.contains("MYD09GA")) {
                  String[] tmp = path.split("/");
                  String p = tmp[tmp.length-1];
                  int timeValue = Integer.parseInt(p.substring(9, 16));
                  if(timeValue<startTime || timeValue>endTime) {
                    System.out.println(file.getPath().toString());
                    nfiles.add(file);
                  }
                } else {

                  String[] paths = path.split("\\.");
                  String time = paths[paths.length - 2];
                  long timeValue = Long.parseLong(time);
                  if (timeValue < startTime || timeValue > endTime) {
                    System.out.println(file.getPath().toString());
                    nfiles.add(file);
                  }
                }
	    }
	    
	    files.removeAll(nfiles);
	    
	    ArrayList<FileStatus> rfiles = new ArrayList<FileStatus>();
	    /*for (FileStatus file: files) {
	       NcHdfsRaf raf = new NcHdfsRaf(file, jobC.getConfiguration());

	       try {
	    	   NetcdfFile ncfile = NetcdfFile.open(raf, file.getPath().toString());
	       } catch (IOException e) {
	    	   LOG.warn("Skipping input: " + e.getMessage());
	    	   rfiles.add(file);
	       }
	    }*/

	    files.removeAll(rfiles);
	    
	    return files;
	  }
	  
	  
	 @Override
	  /**
	   * Creates a RecordReader for combined NetCDF files
	   * @param split The split that this record will be processing
	   * @param context A TaskAttemptContext for the task that will be using
	   * the returned RecordReader
	   * @return A NetCDFRecordReacer 
	   */
	  public RecordReader<ArraySpec, Array> createRecordReader( InputSplit split,
	                                                TaskAttemptContext context )
	                                                throws IOException, InterruptedException { 
	    CombineNetCDFRecordReader reader = new CombineNetCDFRecordReader();
		//reader.initialize( split, context);

	    return reader;
	  }
}
