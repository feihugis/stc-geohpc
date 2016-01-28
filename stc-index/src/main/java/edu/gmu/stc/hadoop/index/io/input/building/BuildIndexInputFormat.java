package edu.gmu.stc.hadoop.index.io.input.building;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BuildIndexInputFormat extends FileInputFormat {
	FileSystem fs = null;
	
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> iptSplitList = new ArrayList<InputSplit>();
		fs = FileSystem.get(job.getConfiguration());
		List<FileStatus> fileStatusList = listStatus(job);
		
		
		String[] variables = job.getConfiguration().getStrings("variables");
		int filesPerMap = Integer.parseInt(job.getConfiguration().get("filesPerMap"));
		
		int count=0;
		BuildIndexSplit bIndexSplit = null;
		for(FileStatus file : fileStatusList) {		
			if(count==0) {
				String[] hosts = fs.getFileBlockLocations(file, 0, file.getLen())[0].getHosts();
				bIndexSplit = new BuildIndexSplit(variables, hosts);
			}
			
			bIndexSplit.addInputFile(file.getPath().toString());
			
			count++;
			
			if(count == filesPerMap) {
				iptSplitList.add(bIndexSplit);
				count = 0;
			}
		}
		
		if(count != 0) {
			iptSplitList.add(bIndexSplit);
		}
		
		return iptSplitList;
	}
	
	/* 
	 * @see org.apache.hadoop.mapreduce.lib.input.FileInputFormat#listStatus(org.apache.hadoop.mapreduce.JobContext)
	 * filter the input files by file format(.hdf), startTime and endTime
	 */
	@Override
	protected List<FileStatus> listStatus(JobContext jobC) throws IOException {

		
		int startTime = Integer.parseInt(jobC.getConfiguration().get("startTime"));
	    int endTime = Integer.parseInt(jobC.getConfiguration().get("endTime"));	
	   
	    List<FileStatus> files = super.listStatus(jobC);
	    
	    //Filter the files that is not HDF format
	    ArrayList<FileStatus> unHDFs = new ArrayList<FileStatus>();
	    for(FileStatus file: files) {
	    	String path = file.getPath().toString();
	    	String[] paths = path.split("\\.");
	    	String format = paths[paths.length-1];
	    	if(!format.equalsIgnoreCase("hdf")) {
	    		unHDFs.add(file);
	    	}	
	    }
	    
	    files.removeAll(unHDFs);
	    
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
	    return files;
	  }

	@Override
	public RecordReader createRecordReader(InputSplit iptSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		BuildIndexRecordReader recordReader = new BuildIndexRecordReader();
		recordReader.initialize(iptSplit, context);
		return recordReader;
	}
	
	

}
