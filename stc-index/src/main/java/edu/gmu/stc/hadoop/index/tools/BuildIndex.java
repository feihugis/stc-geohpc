package edu.gmu.stc.hadoop.index.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Date;

import edu.gmu.stc.configure.MyProperty;
import edu.gmu.stc.hadoop.index.combine.BuildIndexCombiner;
import edu.gmu.stc.hadoop.index.io.input.building.BuildIndexInputFormat;
import edu.gmu.stc.hadoop.index.io.merra.VariableInfo;
import edu.gmu.stc.hadoop.index.map.BuildIndexMapper;
import edu.gmu.stc.hadoop.index.reduce.BuildIndexReducer;


public class BuildIndex extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 6) {
			System.err.println("Usage: identity <input> <output> <variableNames> <filesPerMap> <startTime> <EndTime>"
					+ "for example: /MerraData/Daily/ /Output/BuildIndex/ DMDT_DYN,DMDT_ANA 3 20150101 20150131");
			System.exit(2);
		}

		Configuration conf = getConf();
		conf.set("fs.defaultFS", MyProperty.nameNode);
		conf.set("mapreduce.map.java.opts", "-Xmx1024M");
	    conf.set("mapreduce.reduce.java.opts", "-Xmx1024M");
		
	    Job job = new Job(conf);
	    String jobNameString = "BuildIndex";
	    
	    job.setInputFormatClass(BuildIndexInputFormat.class);
	    BuildIndexInputFormat.addInputPath(job, new Path(args[0]));
		BuildIndexInputFormat.setInputDirRecursive(job, true);
	    
		FileOutputFormat.setOutputPath(job, new Path(args[1] + (new Date()).getTime()));
		
	    String varInput = args[2]; 
	    String[] variables = varInput.split(",");
	    job.getConfiguration().setStrings("variables", variables);
	    job.getConfiguration().setStrings("filesPerMap", args[3]);
	    job.getConfiguration().setStrings("startTime", args[4]);
	    job.getConfiguration().setStrings("endTime", args[5]);
	   
	    // get the buffer size
	    int bufferSize = Utils.getBufferSize(conf);
	  
	    job.setJarByClass(BuildIndex.class);
	    job.setMapperClass(BuildIndexMapper.class);
	    job.setCombinerClass(BuildIndexCombiner.class);
	    job.setReducerClass(BuildIndexReducer.class);
		
		// mapper output  
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VariableInfo.class);
		   
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		  
		//job.setNumReduceTasks( Utils.getNumberReducers(conf) );	  
		
			
		job.setJobName(jobNameString);
		job.waitForCompletion(true);
			
		return 0;
	}
	
	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new BuildIndex(), args);
		System.exit(res);
	}

}
