package edu.gmu.stc.hadoop.index.io.input.building;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class BuildIndexRecordReader extends RecordReader<Text, Text>{
	private BuildIndexSplit bIdxSplit = null;
	private int count=-1;
	private String varsString = "";
	private String filesString = "";
	private String[] vars = null;
	private String[] files = null;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.bIdxSplit = (BuildIndexSplit) split;	
		vars = new String[this.bIdxSplit.getVariables().length];
		files = new String[this.bIdxSplit.getFileList().size()];
		
		varsString = "";
		filesString = "";
		for(int i=0; i<vars.length; i++) {
			vars[i] = this.bIdxSplit.getVariables()[i];
			varsString = varsString + vars[i] + ",";
		}
		
		for(int i=0; i<this.bIdxSplit.getFileList().size(); i++) {
			files[i] = this.bIdxSplit.getFileList().get(i);
			filesString = filesString + this.bIdxSplit.getFileList().get(i) + ",";
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		count++;
		if(count>=this.files.length) {
			return false;
		} else {
			return true;
		}	
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return new Text(this.files[count]);
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return new Text(this.varsString);
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 1.0f*this.count/this.files.length;
	}

	@Override
	public void close() throws IOException {
		
	}

}
