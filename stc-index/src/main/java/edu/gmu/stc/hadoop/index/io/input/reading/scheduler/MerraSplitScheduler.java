package edu.gmu.stc.hadoop.index.io.input.reading.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import edu.gmu.stc.hadoop.index.io.input.reading.ArrayBasedSplitByDBIndex;
import edu.gmu.stc.hadoop.index.io.input.reading.MerraInputFormatByDBIndexWithScheduler;
import edu.gmu.stc.hadoop.index.io.input.reading.MultiArrayBasedSplitByDBIndex;


public class MerraSplitScheduler {
	
	private static final Log LOG = LogFactory.getLog(MerraSplitScheduler.class);
	
	private List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
	private List<String> hostsList = new ArrayList<String>();
	private int datanodeNum = 1;
	private int threadsNumPerNode = 1;
	private int resultNumofSplits = 0;
	
	//private HashMap<String, List<InputSplit>> hostsToInputSplits = new HashMap<String, List<InputSplit>>();
	
	public MerraSplitScheduler(List<InputSplit> inpSplits, int num_thread) {
		this.inputSplitList = new ArrayList<InputSplit>(inpSplits);
				
		this.threadsNumPerNode = num_thread;
		
		this.hostsList = getHostsList(this.inputSplitList);

		String hosts="";

		for (String one : hostsList) {
			hosts = hosts + one + "_";
		}

		LOG.info("++++++++++++++++   The data are stored on " + hostsList.size() + " nodes: " + hosts);

		this.datanodeNum = this.hostsList.size();
		//this.initializeHostsToInputSplits();
	}
	
	public List<String> getHostsList(List<InputSplit> inpSplits) {
		List<String> htsList = new ArrayList<String>();
		
		for(InputSplit inptSplt : inpSplits) {
			try {
				String[] hosts = inptSplt.getLocations();
				for(int i=0; i<hosts.length; i++) {
					if( !htsList.contains(hosts[i]) ) {
						htsList.add(hosts[i]);
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
		
		return htsList;
	}
	
/*	public void initializeHostsToInputSplits() {
		for(String host : this.hostsList) {
			this.hostsToInputSplits.put(host, new ArrayList<InputSplit>());
		}
		this.classifyByHost(this.inputSplitList);
	}

	*//**
	 * @param iptSplitList
	 *
	 * classify the inputsplits by their hosts
	 *//*
	public void classifyByHost(List<InputSplit> iptSplitList) {
		for(InputSplit iptSplit : iptSplitList) {
			try {
				String[] hosts = iptSplit.getLocations();
				for(String host : hosts) {
					this.hostsToInputSplits.get(host).add(iptSplit);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}*/
	
	public List<MultiArrayBasedSplitByDBIndex> reorganizeInputSplit() throws IOException, InterruptedException {
		List<MultiArrayBasedSplitByDBIndex> multiInputSplitList = new ArrayList<MultiArrayBasedSplitByDBIndex>();
		
		HashMap<String, List<InputSplit>> assignments = assignInputSplitToHosts(this.hostsList, this.inputSplitList);
		
		int n = this.inputSplitList.size()/(this.datanodeNum*this.threadsNumPerNode);
		int splitNumPerThread = 1>n? 1:n;
		
		Iterator<Entry<String, List<InputSplit>>> itor = assignments.entrySet().iterator();
		
		while(itor.hasNext()) {
			Entry<String, List<InputSplit>> entry = itor.next();
			String[] hosts = new String[1];
			hosts[0] = entry.getKey();
			splitNumPerThread = entry.getValue().size()/this.threadsNumPerNode;
			
			multiInputSplitList.addAll(combineSplibts(hosts, entry.getValue(), this.threadsNumPerNode));
		}
			
		int num = 0;
		Iterator<MultiArrayBasedSplitByDBIndex> mItor = multiInputSplitList.iterator();
		while(mItor.hasNext()) {
			
			num = mItor.next().getSplitNum();
			if(num <1) {
				itor.remove();
			}
			this.resultNumofSplits += num;
			
		}
		
		LOG.info("**************************  For MerraSplit: " + "InputSplit num is " + this.inputSplitList.size()
					+ " ; OutMultiSplits contain " + this.resultNumofSplits + " InputSplits" );
		
		return multiInputSplitList;
	}
	
	public List<MultiArrayBasedSplitByDBIndex> combineSplibts(String[] hosts, List<InputSplit> inputSplitList, int threadNumPerNode) throws IOException, InterruptedException {
	
		
		List<MultiArrayBasedSplitByDBIndex> multiSplitsList = new ArrayList<MultiArrayBasedSplitByDBIndex>();
		int num = inputSplitList.size();
		int[] splitNumOfThread = new int[threadNumPerNode];
		int unit = num/threadNumPerNode;
		int left = num - unit*threadNumPerNode;
		for(int i=0; i<threadNumPerNode; i++) {
			if(i<left) {
				splitNumOfThread[i] = unit + 1;
			} else {
				splitNumOfThread[i] = unit;
			}
		}
		
		int bCombine = 0, aCombine = 0;
		int start=0, end=0;
		for(int i=0; i<threadNumPerNode; i++) {
			start = end;
			end = start + splitNumOfThread[i];
	
			List<InputSplit> subList = inputSplitList.subList(start, end);
			if(subList.size()<1) {
				continue;
			}
			
			bCombine += subList.size();
			//combine the splits who have the same filePath			
			List<InputSplit> subListCombine = MerraInputFormatByDBIndexWithScheduler.combineInputSplitByFilePath(new ArrayList<InputSplit>(subList));
			aCombine += subListCombine.size();
			
			multiSplitsList.add(new MultiArrayBasedSplitByDBIndex(hosts, subListCombine));
		}
		
		 LOG.info(Arrays.toString(hosts) + "**************************  " + bCombine + " splits have been combined into " + aCombine + " splits \n");
		
		return multiSplitsList;
	}
	
	/**
	 * assign inputSplits to hosts
	 * 
	 * @param hosts
	 * @param iptSplitList
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 */
	public HashMap<String, List<InputSplit>> assignInputSplitToHosts(List<String> hosts, List<InputSplit>inputSplitList) throws IOException, InterruptedException {
		int s = inputSplitList.size()/hosts.size();
		int splitNumPerNode = s>1? s:1;
		List<InputSplit> iptSplitList = new ArrayList<InputSplit>(inputSplitList);
		
		HashMap<String, List<InputSplit>> assignments = new HashMap<String, List<InputSplit>>();
		
		//show the distribution of splits
		int[] splitNum = new int[hosts.size()];
		int[] recordNum = new int[hosts.size()];
		
		for(int i=0; i<splitNum.length; i++) {
			splitNum[i] = 0;
			recordNum[i] = 0;
		}
		for(int i=0; i<hosts.size(); i++) {
			for(InputSplit ipt : inputSplitList) {
				if(isContainHost(ipt, hosts.get(i))) {
					splitNum[i] ++;
					recordNum[i] += ((ArrayBasedSplitByDBIndex) ipt).getVarInfoList().size();
				}
			}
		}
		
		for(int i=0; i<hosts.size(); i++) {
			LOG.info("**************************  " + hosts.get(i) + " have " + splitNum[i] + " splits, " + recordNum[i] + " records" + "\n");
		}
		
		for(String host : hosts) {
			List<Integer> indexForUsedSplits = new ArrayList<Integer>();
			assignments.put(host, new ArrayList<InputSplit>());
			
			//assign the splits to the certain host according to the number of splits a node should be assigned
			for(int i=0; i<iptSplitList.size(); i++) {
				if(isContainHost(iptSplitList.get(i), host)) {
					assignments.get(host).add(iptSplitList.get(i));
					indexForUsedSplits.add(i);
				}
				if(indexForUsedSplits.size()>=splitNumPerNode) {
					break;
				}
			}
			
			//delete those splits who have been assigned
			List<InputSplit> tmp = new ArrayList<InputSplit>(iptSplitList);
			for(Integer i : indexForUsedSplits) {
				//iptSplitList.remove(i);
				tmp.remove(iptSplitList.get(i));
			}	
			
			iptSplitList = new ArrayList<InputSplit>(tmp);
			
			/*Iterator<InputSplit> itor = iptSplitList.iterator();
			Integer count =0;
			while(itor.hasNext()) {
				itor.next();			
				if(indexForUserSplits.contains(count)) {
					itor.remove();
				}
				count++;
			}*/
		}
		
		//assign the left splits by the previous step
		if(iptSplitList.size()>0) {
			int n = iptSplitList.size();
			for(int i=0; i<n; i++) {
				String host = iptSplitList.get(i).getLocations()[0];
				assignments.get(host).add(iptSplitList.get(i));
				/*for(String host : hosts) {
					if(isContainHost(iptSplitList.get(i), host)) {
						assignments.get(host).add(iptSplitList.get(i));
						break;
					}
				}*/
			}
		}
		
		//************************* balance the assignment *************************		
		String unFullHosts = "", fullHosts = "";
		for(String host : hosts) {
			if(assignments.get(host).size() < splitNumPerNode) {
				unFullHosts = unFullHosts + host + ",";
			}		
			if(assignments.get(host).size() >= splitNumPerNode) {
				fullHosts = fullHosts + host + ",";
			}			
		}
		
		String[] ufHosts = unFullHosts.split(",");
		String[] fHosts = fullHosts.split(",");
		for(int i=0; i< ufHosts.length; i++) {
			if(ufHosts[i].equals("")) {
				continue;
			}
			List<InputSplit> tmpSplitList = new ArrayList<InputSplit>();		
			for(int j=0; j< fHosts.length; j++) {
				if(assignments.get(fHosts[j]).size()<=splitNumPerNode) {
					continue;
				}
					
				for(InputSplit ipt : assignments.get(fHosts[j])) {
					if(isContainHost(ipt, ufHosts[i])) {
						assignments.get(ufHosts[i]).add(ipt);
						tmpSplitList.add(ipt);
						if((assignments.get(fHosts[j]).size()-tmpSplitList.size())<=(splitNumPerNode+1) || assignments.get(ufHosts[i]).size()>=splitNumPerNode) {
							break;
						}
					}
				}
					
				for(InputSplit ipt : tmpSplitList) {
					assignments.get(fHosts[j]).remove(ipt);
				}
					
				if(assignments.get(ufHosts[i]).size()>=splitNumPerNode) {
					break;
				}
					
				tmpSplitList = new ArrayList<InputSplit>();			
				}
		}
		/////////////////////////////////////////////////////////////////////////////
		
		Iterator<Entry<String, List<InputSplit>>> itor = assignments.entrySet().iterator();
		while(itor.hasNext()) {
			Entry<String, List<InputSplit>> entry = itor.next();
			int num =0;
			for(InputSplit split : entry.getValue()) {
				num = num + ((ArrayBasedSplitByDBIndex) split).getVarInfoList().size();
			}
			LOG.info("**************************  " + entry.getKey() + " has been assigned " + entry.getValue().size() + " splits, " + num + " records"+ "\n");
		}
		
		return assignments;
		
	}
	
	
	
	public boolean isContainHost(InputSplit inputSplit, String host) throws IOException, InterruptedException {
		String[] hosts = inputSplit.getLocations();
		for(String one : hosts) {
			if(one.equals(host)) {
				return true;
			}
		}
		
		return false;
	}

}
