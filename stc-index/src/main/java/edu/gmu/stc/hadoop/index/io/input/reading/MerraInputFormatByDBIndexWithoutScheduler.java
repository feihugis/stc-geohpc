package edu.gmu.stc.hadoop.index.io.input.reading;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import edu.gmu.stc.database.IndexOperator;
import edu.gmu.stc.hadoop.index.io.merra.ArraySpec;
import edu.gmu.stc.hadoop.index.io.merra.SpatioRange;
import edu.gmu.stc.hadoop.index.io.merra.VariableInfo;
import edu.gmu.stc.hadoop.index.tools.Utils;


public class MerraInputFormatByDBIndexWithoutScheduler extends FileInputFormat {
private static final Log LOG = LogFactory.getLog(MerraInputFormatByDBIndexWithoutScheduler.class);
	
	static float vMax = 1.0E30f;
	static float vMin = -1.0E30f;
	static float fillValue = 9.9999999E14f;

	
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		
		LOG.info("**************************  Start to getSplits");
		long startTime = System.currentTimeMillis();
        int numDims = 0;
		
		List<InputSplit> inputSplits = new ArrayList<InputSplit>();
		IndexOperator indexOptr = new IndexOperator();
		
		List<String> varNamesList = getVariableQuearyList(job.getConfiguration().getStrings("variables"));
		int startDate = Integer.parseInt(job.getConfiguration().get("startTime")); //For daily data, the format should be 20141001 (yyyymmdd)
	    int endDate = Integer.parseInt(job.getConfiguration().get("endTime"));
	      
		String inputBbox = job.getConfiguration().get("bbox");
		HashMap<String, List<Integer[]>> bboxMap = Utils.parseBbox(inputBbox);
		  
		List<Integer[]> startList = new ArrayList<Integer[]>(bboxMap.get("start"));
		List<Integer[]> endList = new ArrayList<Integer[]>(bboxMap.get("end"));
		
		List<ArraySpec> arraySpecList = new ArrayList<ArraySpec>();
		List<SpatioRange> spatioRngList = new ArrayList<SpatioRange>();
		
		//get the query variable info from DB
	    List<VariableInfo> varInfoList = new ArrayList<VariableInfo>();
	    
	    long sT = System.currentTimeMillis();
		for(int i=0; i<startList.size(); i++) {
			   int[] start = new int[startList.get(i).length];
			   int[] end = new int[startList.get(i).length];
			   
			   for(int j=0; j<startList.get(i).length; j++) {
				   start[j] = startList.get(i)[j];
				   end[j] = endList.get(i)[j];		   
			   }
			   
			   int sTime = start[0];
			   int eTime = end[0];

               numDims = end.length - 2;
			   
			   // build spatial range
			  spatioRngList.add(createSpatioRange(start, end));
			   
			   //get variable info from DB
			  
			  for(String varName : varNamesList) {
			    	varInfoList.addAll(indexOptr.getVarInfoByTimeQuery(startDate, endDate, sTime, eTime, varName));
			  }	   
			   //arraySpecList.addAll(createArraySpecs(varNamesList, start, end));	   
		}
		long eT = System.currentTimeMillis();
		LOG.info("**************************  VarInfo query spends " + (eT - sT)*1.0/1000.0 + " seconds");
		
		
		for(SpatioRange sptRng : spatioRngList) {
			arraySpecList.add(new ArraySpec(sptRng.corner, sptRng.shape, this.vMax, this.vMin, this.fillValue));
		}
		
		List<FileStatus> fileStatusList = listStatus(job);
		List<ArrayBasedSplitByDBIndex> arraySplitListbyIndex = new ArrayList<ArrayBasedSplitByDBIndex>();
		
		//intialize inputsplit
		String filePathPrefix = "";
		String filePathEnd = "";
		for(VariableInfo varInfo : varInfoList) {
			String filePath = new String();

            int v = (int) Math.pow(100.00, numDims);
            int time = varInfo.getTime()/v ;

            for(FileStatus fileStatus : fileStatusList) {
                String path = fileStatus.getPath().toString();
                if(path.contains(time+"")){
                    filePath = new String(path);
                    break;
                }
            }

            if(filePath.equals("")) {
                continue;
            }

			/*int time = varInfo.getTime()/100 ;
			
			if(filePathPrefix.equals("") || filePathEnd.equals("")) {
				for(FileStatus fileStatus : fileStatusList) {
					if(fileStatus.getPath().toString().contains(time+"")) {
						filePath = fileStatus.getPath().toString();
					}
				}
				
				String[] tmp = filePath.split("\\.");
				filePathEnd = "." + tmp[tmp.length-1];
				for(int i=0; i<(tmp.length-2); i++) {
					filePathPrefix += tmp[i] + ".";
				}
			} else {
				filePath = filePathPrefix + time + filePathEnd;
			}*/
			
			
			ArrayBasedSplitByDBIndex iptSplit = new ArrayBasedSplitByDBIndex(arraySpecList, varInfo, filePath);
			arraySplitListbyIndex.add(iptSplit);
		}
		
		//List<MultiArrayBasedSplitByDBIndex> multiSplitList = new ArrayList<MultiArrayBasedSplitByDBIndex>();
		//inputSplits.addAll(arraySplitListbyIndex);
		try {
			inputSplits.addAll(combineInputSplitByHostfilePath(arraySplitListbyIndex));	
		} catch (InterruptedException e) {
			e.printStackTrace();
		}                                 
					 
		long endTime = System.currentTimeMillis();
		try {
			gridDistribution(inputSplits);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		LOG.info("**************************  Create " + inputSplits.size() + " Splits spending " + (endTime - startTime)*1.0/1000.0 + " seconds");
		return inputSplits;		  
	}
	
	public List<ArrayBasedSplitByDBIndex> combineInputSplitByHostfilePath(List<ArrayBasedSplitByDBIndex> inputSplitList) throws IOException, InterruptedException {
		List<ArrayBasedSplitByDBIndex> results = new ArrayList<ArrayBasedSplitByDBIndex>();
		int sum = inputSplitList.size();
		for(int i=0; i<sum; i++) {
			boolean isSame= false;
			ArrayBasedSplitByDBIndex inputSplit = inputSplitList.get(0);
			int j=0;
			for(j=0; j<results.size(); j++) {
				ArrayBasedSplitByDBIndex resultSplit = results.get(j);
				if( isSameHosts(inputSplit.getLocations(), resultSplit.getLocations()) && inputSplit.getFilePath().equals(resultSplit.getFilePath()) ) {
					isSame = true;
					break;
				}				
			}
			
			if(isSame) {
				results.get(j).addVarInfo(inputSplit.getVarInfoList());
			} else {
				results.add(inputSplit);
			}
			
			inputSplitList.remove(0);			
		}
		
		return results;
		
	}
	
	public static boolean isSameHosts(String[] hosts1, String[] hosts2) {
		boolean isSame = true;
		
		if(hosts1.length != hosts2.length) {
			isSame = false;
		} else{
			for(int i=0; i<hosts1.length; i++) {
				boolean mark = false;
				for(int j=0; j<hosts2.length; j++) {
					if(hosts1[i].equals(hosts2[j])) {
						mark = true;
					}				
				}
				if(!mark) {
					isSame = false;
					return false;
				}
			}
		}
		return isSame;
	}
	
	public SpatioRange createSpatioRange(int[] startCorner, int[] endCorner) {
		int[] spatioStart = new int[startCorner.length-1];
		int[] spatioEnd = new int[endCorner.length-1];
		int[] spatioShape = new int[startCorner.length-1];
		
		for(int i=0; i<spatioStart.length; i++) {
			spatioStart[i] = startCorner[i+1];
		}
		for(int i=0; i<spatioEnd.length; i++) {
			spatioEnd[i] = endCorner[i+1];
		}
		for(int i=0; i<spatioShape.length; i++) {
			spatioShape[i] = spatioEnd[i] - spatioStart[i];
		}
		
		return new SpatioRange(spatioStart, spatioShape);
	}
	
	public List<ArraySpec> createArraySpecs(List<String> varNameList, int[] startCorner, int[] endCorner) {
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
 		 
		  ArrayList<ArraySpec> arraySpec = new ArrayList<ArraySpec>();
			  
		  for(String varName : varNameList) {
				  for(int i=0; i<time; i = i + shape[0]) {		  	
						try {
							corner[0] = startCorner[0] + i;
							ArraySpec tempSpec = new ArraySpec(corner, shape, varName);
							tempSpec.setLogicalStartOffset(startOffset);
							tempSpec.setVmax(vMax);
							tempSpec.setVmin(vMin);
							tempSpec.setFillValue(fillValue);
							//tempSpec.setBboxType(bboxType);
							arraySpec.add(tempSpec);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}	  
				  }
		  }
			  	 	    	  
		  return arraySpec;
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
	
	@Override
	protected List<FileStatus> listStatus(JobContext jobC) throws IOException {

		int startTime = Integer.parseInt(jobC.getConfiguration().get("startTime"));
	    int endTime = Integer.parseInt(jobC.getConfiguration().get("endTime"));	
	   
	    List<FileStatus> files = super.listStatus(jobC);
	    
	    ArrayList<FileStatus> nfiles = new ArrayList<FileStatus>();
	    for (FileStatus file: files) {
	    	String path = file.getPath().toString();
	    	String[] paths = path.split("\\.");
			String time = paths[paths.length - 2];
			int timeValue = Integer.parseInt(time);
			if(timeValue<startTime || timeValue>endTime) {
				System.out.println(file.getPath().toString());
				nfiles.add(file);
			}
	    }
	    
	    files.removeAll(nfiles);
	    
	   /* ArrayList<FileStatus> rfiles = new ArrayList<FileStatus>();
	    for (FileStatus file: files) {
	       NcHdfsRaf raf = new NcHdfsRaf(file, jobC.getConfiguration());

	       try {
	    	   NetcdfFile ncfile = NetcdfFile.open(raf, file.getPath().toString());
	       } catch (IOException e) {
	    	   LOG.warn("Skipping input: " + e.getMessage());
	    	   rfiles.add(file);
	       }
	    }

	    files.removeAll(rfiles);*/
	    
	    return files;
	  }
	
	public void gridDistribution(List<InputSplit> splitList) throws IOException, InterruptedException {
		HashMap<String, Integer> splitCountMap = new HashMap<String, Integer>();
		
		for(InputSplit iptSplit : splitList) {
			ArrayBasedSplitByDBIndex split = (ArrayBasedSplitByDBIndex) iptSplit;
			if( !splitCountMap.containsKey(split.getLocations()[0])) {
				splitCountMap.put(split.getLocations()[0], new Integer(1));
			} else {
				
				Integer n = splitCountMap.get(iptSplit.getLocations()[0]);
				splitCountMap.remove(iptSplit.getLocations()[0]);
				splitCountMap.put(iptSplit.getLocations()[0], n+1);
			}
		}
		
		Iterator<Entry<String, Integer>> itor = splitCountMap.entrySet().iterator();
		while(itor.hasNext()) {
			Entry<String, Integer> entry = itor.next();
			System.out.println("**********" + entry.getKey() + " has been assigned " + entry.getValue() + " split");
		}
	}

	@Override
	public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		MerraRecordReaderForDBIndex recordReader = new MerraRecordReaderForDBIndex();
		return recordReader;
	}

}
