package edu.gmu.stc.mapreduce.jobs;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.gmu.stc.mapreduce.Utils;
import edu.gmu.stc.mapreduce.combine.AverageFloatSimpleCombiner;
import edu.gmu.stc.mapreduce.io.SumNum;
import edu.gmu.stc.mapreduce.io.input.MerraInputFormatByFileIndexWithScheduler;
import edu.gmu.stc.mapreduce.map.AverageFloatSimpleMapper;
import edu.gmu.stc.mapreduce.reduce.AverageFloatSimpleReducer;


public class AverageFloatSimpleByFileIndexWithScheduler extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		
		if (args.length != 8) {
			System.err.println("Usage: identity <input> <output> <variableNames> <BBox><startTime><endTime><dataNodeNum><slotNum> "
					+ "for example: /Users/feihu/Documents/Data/Merra/ /Users/feihu/Documents/Data/Output GRN [0-1,0-361,0-540],[0-1,0-361,0-540] 20141001 20141002 4 6 ");
			System.exit(2);
		}

	Configuration conf = getConf();
	//conf.set("fs.defaultFS", MyProperty.nameNode);
	
	/*System.setProperty("HADOOP_USER_NAME","hdfs");	    
	conf.set("hadoop.job.ugi", "supergroup");
    
	conf.set("mapreduce.framework.name", "yarn");
	conf.set("fs.defaultFS", "hdfs://compute-04:8020");
	conf.set("mapreduce.map.java.opts", "-Xmx1024M");
	conf.set("mapreduce.reduce.java.opts", "-Xmx1024M");
	conf.set("mapreduce.jobhistory.address", "compute-04:10020");
    
	conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx1024m");
    
	conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
	conf.set("yarn.resourcemanager.address", "199.26.254.134:8032");
    
	conf.set("yarn.resourcemanager.resource-tracker.address", "199.26.254.134:8031");
	conf.set("yarn.resourcemanager.scheduler.address", "199.26.254.134:8030");
	conf.set("yarn.resourcemanager.admin.address", "199.26.254.134:8033");
    
    
	conf.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");
    
	conf.set("yarn.application.classpath", "/etc/hadoop/conf.cloudera.hdfs,"
    		+ "/etc/hadoop/conf.cloudera.yarn,"
    		+ "/opt/cloudera/parcels/CDH-5.4.4-1.cdh5.4.4.p0.4/lib/hadoop/*,"
    		+ "/opt/cloudera/parcels/CDH-5.4.4-1.cdh5.4.4.p0.4/lib/hadoop/lib/*,"
    		+ "/opt/cloudera/parcels/CDH-5.4.4-1.cdh5.4.4.p0.4/lib/hadoop-hdfs/*,"
    		+ "/opt/cloudera/parcels/CDH-5.4.4-1.cdh5.4.4.p0.4/lib/hadoop-hdfs/lib/*,"
    		+ "/opt/cloudera/parcels/CDH-5.4.4-1.cdh5.4.4.p0.4/lib/hadoop-yarn/*,"
    		+ "/opt/cloudera/parcels/CDH-5.4.4-1.cdh5.4.4.p0.4/lib/hadoop-yarn/lib/*");
    */
    Job job = new Job(conf);
    String jobNameString = "";
    
    String varInput = args[2];// "EOSGRID/Data_Fields/CPT,EOSGRID/Data_Fields/KE";
    String[] variables = varInput.split(","); 
    job.getConfiguration().setStrings("variables", variables);
    
    job.getConfiguration().setStrings("bbox", args[3]);
    job.getConfiguration().setStrings("startTime", args[4]);
    job.getConfiguration().setStrings("endTime", args[5]);
    job.getConfiguration().setStrings("datanodeNum", args[6]);
    job.getConfiguration().setStrings("slotNum", args[7]);

    // get the buffer size
    int bufferSize = Utils.getBufferSize(conf);
    //jobNameString += " buffersize: " + bufferSize + " ";

    jobNameString += "AverageByFileIndexWithScheduler" + ":" + args[4] + " -- " + args[5] + " , " + variables.length + " variables";
    job.setJarByClass(AverageFloatSimpleByFileIndexWithScheduler.class);
    
    job.setMapperClass(AverageFloatSimpleMapper.class);

    if ( Utils.useCombiner(conf) ) {
      jobNameString += " with combiner ";
      job.setCombinerClass(AverageFloatSimpleCombiner.class);
    }
          job.setReducerClass(AverageFloatSimpleReducer.class);

          // mapper output
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(SumNum.class);
	 
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(FloatWritable.class);
	  
	job.setInputFormatClass(MerraInputFormatByFileIndexWithScheduler.class);
	job.setNumReduceTasks( Utils.getNumberReducers(conf) );
	    
	MerraInputFormatByFileIndexWithScheduler.addInputPath(job, new Path(args[0]));
          FileOutputFormat.setOutputPath(job, new Path(args[1] + (new Date()).getTime()));

	
    job.setJobName(jobNameString);
    job.waitForCompletion(true);
    
    return 0;
        }
	

    public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new AverageFloatSimpleByFileIndexWithScheduler(), args);
		System.exit(res);

    }
}
