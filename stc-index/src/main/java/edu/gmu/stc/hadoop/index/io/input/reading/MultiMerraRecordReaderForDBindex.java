package edu.gmu.stc.hadoop.index.io.input.reading;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.hadoop.index.io.merra.VariableInfo;
import ucar.ma2.Array;

public class MultiMerraRecordReaderForDBindex extends RecordReader<VariableInfo, Array> {
	private static final Log LOG = LogFactory.getLog(MultiMerraRecordReaderForDBindex.class);
	
	private MultiArrayBasedSplitByDBIndex multiInputSplit;
	private TaskAttemptContext context;
	private List<ArrayBasedSplitByDBIndex> arrayInputSplit;// = new ArrayList<ArrayBasedSplitByDBIndex>();
	private int keyNum = 0;
	private int countPerKey = 0;
	private int totalNum;
	private MerraRecordReaderForDBIndex currentMerraRecordReader;
	private int totalDataNum = 0;
	private int localDataNum = 0;

	@Override
	public void initialize(InputSplit inputSplits, TaskAttemptContext context) throws IOException, InterruptedException {
		this.multiInputSplit = (MultiArrayBasedSplitByDBIndex) inputSplits;
		this.context = context;
		this.arrayInputSplit = new ArrayList<ArrayBasedSplitByDBIndex>(multiInputSplit.getInputSplitList());
		this.totalNum = this.arrayInputSplit.size();
		this.currentMerraRecordReader = new MerraRecordReaderForDBIndex();
		this.currentMerraRecordReader.initialize(this.arrayInputSplit.get(0), context);
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public VariableInfo getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return this.currentMerraRecordReader.getCurrentKey();
	}

	@Override
	public Array getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return this.currentMerraRecordReader.getCurrentValue();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return (this.currentMerraRecordReader.getProgress() + keyNum)/(this.totalNum*1.0f);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		boolean hasnext = this.currentMerraRecordReader.nextKeyValue();
		
		if( !hasnext ) {
			this.totalDataNum = this.totalDataNum + this.currentMerraRecordReader.getTotalDataNum();
			this.localDataNum = this.localDataNum + this.currentMerraRecordReader.getLocalDataNum();
			this.keyNum++;
			if(keyNum == this.totalNum) {
				this.context.getConfiguration().set("localData", this.localDataNum+"");
				this.context.getConfiguration().set("totalData", this.totalDataNum+"");
				return false;
			} else {
				this.currentMerraRecordReader = new MerraRecordReaderForDBIndex();
				this.currentMerraRecordReader.initialize(this.arrayInputSplit.get(keyNum), this.context);
				this.currentMerraRecordReader.nextKeyValue();
				return true;
			}
		}
		
		return hasnext;
	}

}
