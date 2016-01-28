package edu.gmu.stc.mapreduce.io.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.gmu.stc.mapreduce.io.ArraySpec;
import ucar.ma2.Array;


public class CombineNetCDFRecordReader extends RecordReader<ArraySpec, Array> {
	private static final Log LOG = LogFactory.getLog(CombineNetCDFRecordReader.class);
    private List<NetCDFRecordReader> recordList = new ArrayList<NetCDFRecordReader>(); 
    private ArraySpec _currentKey = null; // this also serves as key
    private Array _currentValue = null;
    private NetCDFRecordReader _currentRecord = new NetCDFRecordReader();
    // how many data elements were read the last step 
    private long _totalDataElements = 1; 

    // how many data elements have been read so far (used to track work done)
    private long _elementsSeenSoFar = 0;
	
	
	@Override
	public void initialize(InputSplit paramInputSplit, TaskAttemptContext paramTaskAttemptContext) 
							throws IOException, InterruptedException {
		CombineArrayBasedFileSplit combinedSplit = (CombineArrayBasedFileSplit) paramInputSplit;
		List<ArrayBasedFileSplit> arraySplitList = new ArrayList<ArrayBasedFileSplit>(combinedSplit.getArrayBasedFileSplitList());
		recordList = new ArrayList<NetCDFRecordReader>();
		long sTime = System.currentTimeMillis();
		for(int i=0; i<arraySplitList.size(); i++) {
			NetCDFRecordReader recordReader = new NetCDFRecordReader();
			recordReader.initialize(arraySplitList.get(i), paramTaskAttemptContext);
			this.recordList.add(recordReader);
		}	
		long eTime = System.currentTimeMillis();
	    LOG.info("*******************************   Fs open file : " + (eTime - sTime)*1.0/1000.0 + " seconds");
		
		this._totalDataElements = this.recordList.size();
		this._currentRecord = this.recordList.get(0);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean isNext = false;
		
		if(recordList.isEmpty()) {
			isNext = false;
		}else {
			this._currentRecord = recordList.get(0);
			
			if(this._currentRecord.nextKeyValue()) {
				isNext=true;
			} else {
				this._currentRecord.close();
				recordList.remove(0);
				this._elementsSeenSoFar++;
				
				if(recordList.isEmpty()) {
					isNext = false;
				} else {
					this._currentRecord = recordList.get(0);
					this._currentRecord.nextKeyValue();
					isNext = true;
				}
			}
		}
		
		return isNext;
	}

	@Override
	public ArraySpec getCurrentKey() throws IOException, InterruptedException {
		
		this._currentKey = this._currentRecord.getCurrentKey();
		return this._currentKey;
	}

	@Override
	public Array getCurrentValue() throws IOException, InterruptedException {
		
		this._currentValue = this._currentRecord.getCurrentValue();
		return this._currentValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
	
		return (float)(this._elementsSeenSoFar / this._totalDataElements + 1.0 / this._totalDataElements * this._currentRecord.getProgress());
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
