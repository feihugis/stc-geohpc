package edu.gmu.stc.hadoop.index.io.input.reading;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.hadoop.index.io.merra.ArraySpec;
import edu.gmu.stc.hadoop.index.io.merra.VariableInfo;

public class ArrayBasedSplitByDBIndex extends InputSplit implements Writable{
	private String[] hosts = null;
	private String filePath = null;
	
	private List<ArraySpec> arraySpecList = new ArrayList<ArraySpec>();
	
	private List<VariableInfo> varInfoList = new ArrayList<VariableInfo>(); //the varInfo in the list should have the same hosts
	
	public ArrayBasedSplitByDBIndex() {
		
	}
	
	public ArrayBasedSplitByDBIndex(List<ArraySpec> arraySpecList, List<VariableInfo> varInfoList, String filePath) {
			this.arraySpecList.addAll(arraySpecList);
			this.varInfoList.addAll(varInfoList);
			
			this.filePath = filePath;
			
			String hosts = this.varInfoList.get(0).getBlockHosts();
			String[] tmpHosts = hosts.split(" ");
			
			this.hosts = new String[tmpHosts.length-2];
			for(int i=0; i<this.hosts.length; i++) {
				this.hosts[i] = tmpHosts[i+1];
			}
	}
	
	public ArrayBasedSplitByDBIndex(List<ArraySpec> arraySpecList, VariableInfo varInfo, String filePath) {
		this.arraySpecList.addAll(arraySpecList);
		this.varInfoList.add(varInfo);
		
		this.filePath = filePath;
		
		String hosts = this.varInfoList.get(0).getBlockHosts();
		String[] tmpHosts = hosts.split(" ");
		
		this.hosts = new String[tmpHosts.length-2];
		for(int i=0; i<this.hosts.length; i++) {
			this.hosts[i] = tmpHosts[i+1];
		}		
	}
	
	public String getFilePath() {
		return this.filePath;
	}
	
	public List<VariableInfo> getVarInfoList() {
		return this.varInfoList;
	}
	
	public List<ArraySpec> getArraySpecList() {
		return this.arraySpecList;
	}
	
	public void addVarInfo(VariableInfo varInfo) {
		this.varInfoList.add(varInfo);
	}
	
	public void addVarInfo(List<VariableInfo> varInfoList) {
		this.varInfoList.addAll(varInfoList);
	}
	
	public void addArraySpec(ArraySpec arraySpec) {
		this.arraySpecList.add(arraySpec);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		int hostsNum = this.hosts.length;
		out.writeInt(hostsNum);
		
		for(int i=0; i<hostsNum; i++) {
			Text.writeString(out, this.hosts[i]);
		}
		
		Text.writeString(out, this.filePath);
		
		out.writeInt(this.arraySpecList.size());
		
		for(ArraySpec arraySpec : arraySpecList) {
			arraySpec.write(out);
		}
		
		out.writeInt(this.varInfoList.size());
		
		for(VariableInfo varInfo : varInfoList) {
			varInfo.write(out);
		}		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int hostsNum = in.readInt();		
		this.hosts = new String[hostsNum];
		for(int i=0; i<hostsNum; i++) {
			this.hosts[i] = new String(Text.readString(in));
		}
		
		this.filePath = new String(Text.readString(in));
		
		int arraySpecNum = in.readInt();
		
		for(int i=0; i<arraySpecNum; i++) {
			ArraySpec arySpec = new ArraySpec();
			arySpec.readFields(in);
			this.arraySpecList.add(arySpec);
		}
		
		int varInfoNum = in.readInt();
		
		for(int i=0; i<varInfoNum; i++) {
			VariableInfo varInfo = new VariableInfo();
			varInfo.readFields(in);
			this.varInfoList.add(varInfo);
		}	
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return this.varInfoList.size();
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return this.hosts;
	}
	
	public boolean isSameLocationfilePath(ArrayBasedSplitByDBIndex inputSplit) throws IOException, InterruptedException {
		if( this.filePath.equals(inputSplit.getFilePath()) 
			&& MerraInputFormatByDBIndexWithScheduler.isSameHosts(this.hosts, inputSplit.getLocations())) {
				return true;
			} else {
				return false;
			}				
	}	
}
