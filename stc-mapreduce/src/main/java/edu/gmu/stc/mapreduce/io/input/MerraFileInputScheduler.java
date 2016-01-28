package edu.gmu.stc.mapreduce.io.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * @author feihu
 * 
 * The class is to reorganize the initial inputSplits to generate a reasonable number of mappers on the right dananodes
 *
 */
public class MerraFileInputScheduler {
	
	private static final Log LOG = LogFactory.getLog(MerraFileInputScheduler.class);
	
	private List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
	private List<String> hostsList = new ArrayList<String>();
	private int datanodeNum = 1;
	private int threadsNum = 1;
	private HashMap<String, List<Path>> hostsToFilePaths = new HashMap<String, List<Path>>();
	private HashMap<String, Integer> fileNumToDataNode = new HashMap<String, Integer>();
	private HashMap<Path, ArrayBasedFileSplit> pathsToArrayBasedFileSplit = new HashMap<Path, ArrayBasedFileSplit>();
	
	private HashMap<String, List<Path>> arrangementResult = new HashMap<String, List<Path>>();

	
	/**
	 * @param inpSplits
	 * 
	 * constructor
	 */
	public MerraFileInputScheduler(List<InputSplit> inpSplits, int num_dataNode, int num_thread) {
		this.inputSplitList = new ArrayList<InputSplit>(inpSplits);
		
		this.datanodeNum = num_dataNode;
		this.threadsNum = num_thread;
		
		initialize(this.inputSplitList);	
	}
	
	/**
	 * @param inputSplitList
	 * 
	 * Initialize the hostsList and hostsToFilePaths
	 */
	public void initialize( List<InputSplit> inputSplitList) {
		
		for(InputSplit splt : inputSplitList) {
			ArrayBasedFileSplit split = (ArrayBasedFileSplit) splt;
			this.pathsToArrayBasedFileSplit.put(split.getPath(), split);
			
			try {
				String[] hosts = new String[split.getLocations().length];
				for(int i=0; i<hosts.length; i++) {
					hosts[i] = split.getLocations()[i];
				}
				
				for(int i=0; i<hosts.length; i++) {
					
					if( !this.hostsList.contains(hosts[i]) ) {
						this.hostsList.add(hosts[i]);		
						
						this.hostsToFilePaths.put(hosts[i], new ArrayList<Path>());
						this.arrangementResult.put(hosts[i], new ArrayList<Path>());					
						this.hostsToFilePaths.get(hosts[i]).add(split.getPath());
						
					} else {
						this.hostsToFilePaths.get(hosts[i]).add(split.getPath());
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	 }
	
	
	/**
	 * @return InputSplit
	 * 
	 * reorganize the initial inputSplits
	 */
	public List<CombineArrayBasedFileSplit> reOrganizeFileInput() {
		List<CombineArrayBasedFileSplit> inputSplits = new ArrayList<CombineArrayBasedFileSplit>();
		
		this.fileNumToDataNode = arrangeFileNumPerDatanode(this.inputSplitList.size(), this.datanodeNum);
		arrangeFilesPerDatanode();
		
		Iterator<Entry<String, List<Path>>> itor = arrangementResult.entrySet().iterator();
		while(itor.hasNext()) {
			Entry<String, List<Path>> entry = itor.next();
		
			String[] inputHost = new String[1];
			inputHost[0] = entry.getKey();
			List<Path> filePaths = new ArrayList<Path>(entry.getValue());
			int[] fileNumPerThread = arrangeFileNumPerThread(filePaths.size(), this.threadsNum);
			int count = 0;
			for(int i=0; i<this.threadsNum; i++) {
				List<ArrayBasedFileSplit> arraySplitList = new ArrayList<ArrayBasedFileSplit>();
				for(int j=count; j<count+fileNumPerThread[i]; j++) {
					if(fileNumPerThread[i]>0) {
						arraySplitList.add(pathsToArrayBasedFileSplit.get(filePaths.get(j)));
					}	
				}	
				
				if(fileNumPerThread[i]>0) {
					inputSplits.add(new CombineArrayBasedFileSplit(arraySplitList, inputHost, i));
				}		
				count = count+fileNumPerThread[i];
			}	
		}	
		
		return inputSplits;
	}
	
	/**
	 * @return the file number per dataNode to deal with
	 * 
	 * get the number of input files each datanode need to handle
	 */
	public HashMap<String, Integer> arrangeFileNumPerDatanode(int allInputFileNum, int datanodeNum) {
		HashMap<String, Integer> fileNumToDataNode = new HashMap<String, Integer>();
		
		int sum = allInputFileNum;
		int firstNum = sum / datanodeNum;
		int left = sum % datanodeNum;
		
		for(int i=0; i<this.hostsList.size(); i++) {
			//Here just arrange the left files to the 0~left datanode, but we could arrange them according to same other factors
			//TODO make the arrangement strategy more reasonable
			fileNumToDataNode.put(hostsList.get(i), new Integer(firstNum + (i<left? 1:0)));					
		}
		
		return fileNumToDataNode;		
	}
	
	public int[] arrangeFileNumPerThread(int inputFileNum, int threadNum) {
		int[] result = new int[threadNum];
		int sum = inputFileNum;
		int firstNum = sum / threadNum;
		int left = sum % threadNum;
		
		for(int i=0; i<threadNum; i++) {
			result[i] = firstNum + ( i < left? 1 : 0);
		}
		
		return result;
	}
	
	/**
	 * allocate each file to the responding node
	 */
	public void arrangeFilesPerDatanode() {
		
		List<InputSplit> inputSplitList = new ArrayList<InputSplit>(this.inputSplitList);
		int sum = inputSplitList.size();
		
		List<InputSplit> inputSplitLeftList = new ArrayList<InputSplit>();
		
		for(int i=0; i<sum; i++) {
			ArrayBasedFileSplit inputSplit = (ArrayBasedFileSplit) inputSplitList.get(0);
			String host = arrangeInput2Host(inputSplit);
			if(! host.isEmpty()) {
				arrangementResult.get(host).add(inputSplit.getPath());
			} else {
				LOG.info("Can not arrange the file" + inputSplit.getPath().toString() + " to a certain host");
				inputSplitLeftList.add(inputSplit);
			}
			
			inputSplitList.remove(0);		
		}
		
		if( inputSplitLeftList.size() > 0) {
			LOG.info("There are some input files left");
			for(int i=0; i<inputSplitLeftList.size(); i++) {
				for(int j=0; j<this.hostsList.size(); j++) {
					String ht = this.hostsList.get(j);
					if(!checkIsFullForArrangement(ht)) {
						arrangementResult.get(ht).add(((ArrayBasedFileSplit) inputSplitLeftList.get(i)).getPath());
					}
				}
			}
		}
		
		
		
	}
	
	public String arrangeInput2Host(ArrayBasedFileSplit inputSplit) {
		String host = new String();
		int sum = this.hostsList.size();
		List<String> candidates = new ArrayList<String>();
		
		for(int i=0; i<sum; i++) {
			String hst = this.hostsList.get(i);
			
			if(this.hostsToFilePaths.get(hst).contains(inputSplit.getPath()) && !checkIsFullForArrangement(hst)) {
				candidates.add(hst);
			}
		}
		
		int max = Integer.MAX_VALUE;
		
		for(String one : candidates) {
			if(arrangementResult.get(one).size() < max) {
				max = arrangementResult.get(one).size();
				host = one;
			}
		}

		return host;
	}
	
	/**
	 * @param host
	 * @return
	 * 
	 * check whether the referred dataNode has been arranged the maximum number of input file
	 */
	public boolean checkIsFullForArrangement(String host) {
		if(this.arrangementResult.get(host).size() < this.fileNumToDataNode.get(host)) {
			return false;
		} else {
			return true;
		}
	}
	
	/**
	 * @return the hosts of the initial inputSplits
	 */
	public List<String> getHostsList(List<InputSplit> inputSplitList) {
		List<String> hostsList = new ArrayList<String>();
		for(InputSplit splt : inputSplitList) {
			ArrayBasedFileSplit split = (ArrayBasedFileSplit) splt;
			try {
				String[] hosts = new String[split.getLocations().length];
				for(int i=0; i<hosts.length; i++) {
					if( !hostsList.contains(hosts[i]) ) {
						hostsList.add(hosts[i]);
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return hostsList;
	}
	
	/**
	 * Combine two lists of CombineArrayBasedFileSplits
	 * @param inputSplit_1_List
	 * @param inputSplit_2_List
	 * @return
	 */
	public static List<CombineArrayBasedFileSplit> combineInputSplit(List<CombineArrayBasedFileSplit> inputSplit_1_List, 
																	 List<CombineArrayBasedFileSplit> inputSplit_2_List	) {
		List<CombineArrayBasedFileSplit> aList = new ArrayList<CombineArrayBasedFileSplit>(inputSplit_1_List);
		List<CombineArrayBasedFileSplit> bList = new ArrayList<CombineArrayBasedFileSplit>(inputSplit_2_List);
		
		if(aList.isEmpty()) {
			aList.addAll(bList);
			return aList;
		}
		
		for(int i=0; i<bList.size(); i++) {
			String b_hostName =  bList.get(i).getHostName()[0];
			int b_threadID = bList.get(i).getThreadID();
			
			boolean isAdd = false;
			
			for(int j=0; j<aList.size(); j++) {
				String a_hostName = aList.get(j).getHostName()[0];
				int a_threadID = aList.get(j).getThreadID();
				
				if(b_hostName.equals(a_hostName) && b_threadID==a_threadID) {
					aList.get(j).getArrayBasedFileSplitList().addAll(bList.get(i).getArrayBasedFileSplitList());
					isAdd = true;
					break;
				}
			}	
			
			if(!isAdd) {
				int n = (i > (aList.size()-1)? (aList.size()-1) : i); 
				aList.get(n).getArrayBasedFileSplitList().addAll(bList.get(n).getArrayBasedFileSplitList());
			}
			
		}
		
		return aList;
		
	}
		
	
	
	

}
