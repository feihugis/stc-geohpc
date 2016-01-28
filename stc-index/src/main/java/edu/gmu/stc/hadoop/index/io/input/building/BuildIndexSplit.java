package edu.gmu.stc.hadoop.index.io.input.building;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author feihu
 * 
 * InputSplit for building index
 *
 */
public class BuildIndexSplit extends InputSplit implements Writable{
	private List<String> fileList = new ArrayList<String>();
	private String[] variables = null;
	private String[] hosts = null;
	
	public BuildIndexSplit() {

	}
	
	public BuildIndexSplit(String[] vars, String[] hs) {
		variables = new String[vars.length];
		hosts = new String[hs.length];
		
		for(int i=0; i<variables.length; i++) {
			variables[i] = vars[i];
		}
		for(int i=0; i<hs.length; i++) {
			hosts[i] = hs[i];
		}	
	}
	
	public void addInputFile(String fileName) {
		this.fileList.add(fileName);
	}
	
	public List<String> getFileList() {
		return this.fileList;
	}
	
	public String[] getVariables() {
		return this.variables;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(fileList.size());
		for(int i=0; i<fileList.size(); i++) {
			Text.writeString(out, fileList.get(i));
		}
		
		out.writeInt(this.hosts.length);
		for(int i=0; i<this.hosts.length; i++) {
			Text.writeString(out, this.hosts[i]);
		}
		
		out.writeInt(variables.length);
		for(int i=0; i<this.variables.length; i++) {
			Text.writeString(out, this.variables[i]);
		}	
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int fileNum = in.readInt();
		this.fileList = new ArrayList<String>();
		for(int i=0; i<fileNum; i++) {
			this.fileList.add(Text.readString(in));
		}
		
		int hostNum = in.readInt();
		this.hosts = new String[hostNum];
		for(int i=0; i<hostNum; i++) {
			this.hosts[i] = Text.readString(in);
		}
		
		int varNum = in.readInt();
		this.variables = new String[varNum];
		for(int i=0; i<varNum; i++) {
			this.variables[i] = Text.readString(in);
		}
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return this.fileList.size();
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return this.hosts;
	}

}
