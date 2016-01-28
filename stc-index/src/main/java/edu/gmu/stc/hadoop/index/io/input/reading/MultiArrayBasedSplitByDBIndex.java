package edu.gmu.stc.hadoop.index.io.input.reading;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiArrayBasedSplitByDBIndex extends InputSplit implements Writable{
	private String[] hosts = null;
	private List<ArrayBasedSplitByDBIndex> inputSplitList = null;
	
	public MultiArrayBasedSplitByDBIndex() {
		
	}
	
	public MultiArrayBasedSplitByDBIndex(String[] hosts, List<InputSplit> splitList) {
		this.hosts = new String[hosts.length];
		for(int i=0; i<this.hosts.length; i++) {
			this.hosts[i] = hosts[i];
		}
		
		this.inputSplitList = new ArrayList<ArrayBasedSplitByDBIndex>();
		
		for(int i=0; i<splitList.size(); i++) {
			ArrayBasedSplitByDBIndex arraySplit = (ArrayBasedSplitByDBIndex) splitList.get(i);
			this.inputSplitList.add(arraySplit);
		}
		
	}
	
	public MultiArrayBasedSplitByDBIndex(List<ArrayBasedSplitByDBIndex> splitList, String[] hosts) {
		this.hosts = new String[hosts.length];
		for(int i=0; i<this.hosts.length; i++) {
			this.hosts[i] = hosts[i];
		}
		
		this.inputSplitList = new ArrayList<ArrayBasedSplitByDBIndex>(splitList);
	}
	
	public int getSplitNum() {
		return this.inputSplitList.size();
	}
	
	public List<ArrayBasedSplitByDBIndex> getInputSplitList() {
		return this.inputSplitList;
	}
	

	@Override
	public long getLength() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return this.inputSplitList.size();
	}
	
	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return this.hosts;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		int numHosts = this.hosts.length;
		out.writeInt(numHosts);
		for(int i=0; i<numHosts; i++) {
			Text.writeString(out, this.hosts[i]);
		}
		
		int numMerraInputSplit = this.inputSplitList.size();
		out.writeInt(numMerraInputSplit);
		for(ArrayBasedSplitByDBIndex split : this.inputSplitList) {
			split.write(out);
		}
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int numHosts = in.readInt();
		this.hosts = new String[numHosts];
		for(int i=0; i<numHosts; i++) {
			this.hosts[i] = new String(Text.readString(in));
		}
		
		this.inputSplitList = new ArrayList<ArrayBasedSplitByDBIndex>();
		int numInputSplit = in.readInt();
		for(int i=0; i<numInputSplit; i++) {
			ArrayBasedSplitByDBIndex inputSplit = new ArrayBasedSplitByDBIndex();
			inputSplit.readFields(in);
			this.inputSplitList.add(inputSplit);
		}
		
	}

}
