package edu.gmu.stc.mapreduce.io.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * @author feihu
 *
 *Combine small inputSplit
 */
public class CombineArrayBasedFileSplit extends InputSplit implements Writable{
	private static final Log LOG = LogFactory.getLog(CombineArrayBasedFileSplit.class);
	
    private List<ArrayBasedFileSplit> multiArrayBasedFileSplit = new ArrayList<ArrayBasedFileSplit>();
    private String[] hosts = null;
    private int splitNum = 0;
    private int threadID = 0;
    
    public CombineArrayBasedFileSplit() {
    	
    }
    
    /**
     * @param inputSplitList
     * @param inputHosts
     * 
     * constructor
     */
    public CombineArrayBasedFileSplit( List<ArrayBasedFileSplit> inputSplitList, String[] inputHosts) {
		// TODO Auto-generated constructor stub
    	this.multiArrayBasedFileSplit = new ArrayList<ArrayBasedFileSplit>(inputSplitList);
    	this.hosts = new String[inputHosts.length];
    	this.splitNum = this.multiArrayBasedFileSplit.size();
    	
    	for(int i=0; i<this.hosts.length; i++) {
    		this.hosts[i] = inputHosts[i];
    	}
	}
    
    /**
     * @param inputSplitList
     * @param inputHosts
     * @param threadID  the thread to run the split on a datanode
     * 
     * constructor
     */
    public CombineArrayBasedFileSplit( List<ArrayBasedFileSplit> inputSplitList, String[] inputHosts, int threadID) {
		// TODO Auto-generated constructor stub
    	this.multiArrayBasedFileSplit = new ArrayList<ArrayBasedFileSplit>(inputSplitList);
    	this.hosts = new String[inputHosts.length];
    	this.splitNum = this.multiArrayBasedFileSplit.size();
    	this.threadID = threadID;
    	
    	for(int i=0; i<this.hosts.length; i++) {
    		this.hosts[i] = inputHosts[i];
    	}
	}
    
	@Override
	public void readFields(DataInput in) throws IOException {
		this.splitNum = in.readInt();
		
		for(int i=0; i<this.splitNum; i++) {
			ArrayBasedFileSplit split = new ArrayBasedFileSplit();
			split.readFields(in);
			this.multiArrayBasedFileSplit.add(split);
		}
		
		int length = in.readInt();
		
		this.hosts = new String[length];
		for(int i=0; i<length; i++) {
			this.hosts[i] = new String(Text.readString(in));
			LOG.info("hosts Name : " + this.hosts[i]);
		}
		
		this.threadID = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.splitNum = this.multiArrayBasedFileSplit.size();
		out.writeInt(this.splitNum);
		
		for(int i=0; i<this.multiArrayBasedFileSplit.size(); i++) {
			multiArrayBasedFileSplit.get(i).write(out);
		}	
		
		out.writeInt(this.hosts.length);
		
		for(int i=0; i<this.hosts.length; i++) {
			Text.writeString(out, this.hosts[i]);
		}
			
		out.writeInt(this.threadID);
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		long length = 0;
		for(int i=0; i<this.multiArrayBasedFileSplit.size(); i++) {
			length = length + this.multiArrayBasedFileSplit.get(i).getLength();
		}
		return length;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		
		if (null == this.hosts)
		      return new String[]{};
		    else 
		      return this.hosts;
	}
	
	public List<ArrayBasedFileSplit> getArrayBasedFileSplitList() {
		return this.multiArrayBasedFileSplit;
	}
	
	public void setThreadID(int id) {
		this.threadID = id;
	}
	
	public int getThreadID() {
		return this.threadID;
	}
	
	public String[] getHostName() {
		return this.hosts;
	}

}
