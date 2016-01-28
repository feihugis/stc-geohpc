package edu.gmu.stc.hadoop.index.io.input.reading;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.hadoop.index.io.merra.ArraySpec;
import edu.gmu.stc.hadoop.index.io.merra.SpatioRange;
import edu.gmu.stc.hadoop.index.io.merra.VariableInfo;
import ucar.ma2.Array;
import ucar.nc2.util.IO;


public class MerraRecordReaderForDBIndex extends RecordReader<VariableInfo, Array> {
	
	private static final Log LOG = LogFactory.getLog(MerraRecordReaderForDBIndex.class);
	
	Configuration conf;
	private ArrayBasedSplitByDBIndex arrayInputSplit;
	private FSDataInputStream inputStream;
	private FileSystem fs;
	private int keyNum = -1;
	private int[] elemPos;
	private int totalElems = 0;
	private int xSize = 540;
	private int ySize = 361;
	private int totalDataNum = 0;
	private int localDataNum = 0;
	private String hostname = null;

	@Override
	public void initialize(InputSplit paramInputSplit, TaskAttemptContext context) throws IOException,
																					      InterruptedException {
		this.arrayInputSplit = (ArrayBasedSplitByDBIndex) paramInputSplit;
		this.conf = context.getConfiguration();
        /*this.conf.addResource(new Path(MyProperty.hadoopHome+"/core-site.xml"));
        this.conf.addResource(new Path(MyProperty.hadoopHome+"/hdfs-site.xml"));

        this.conf.set("fs.defaultFS", MyProperty.nameNode);
        this.conf.set("mapreduce.map.java.opts", "-Xmx1024M");
        this. conf.set("mapreduce.reduce.java.opts", "-Xmx1024M");

        this.conf.set("mapreduce.framework.name", "yarn");
        //this.conf.set("fs.defaultFS", "hdfs://SERVER-A8-C-U26:8020");
        //this.conf.set("mapreduce.map.java.opts", "-Xmx1024M");
        //this.conf.set("mapreduce.reduce.java.opts", "-Xmx1024M");
        this.conf.set("mapreduce.jobhistory.address", "SERVER-A8-C-U26:10020");

        this.conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx1024m");

        this.conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        this.conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        this.conf.set("yarn.resourcemanager.address", "10.8.2.26:8032");

        this.conf.set("yarn.resourcemanager.resource-tracker.address", "10.8.2.26:8031");
        this.conf.set("yarn.resourcemanager.scheduler.address", "10.8.2.26:8030");
        this.conf.set("yarn.resourcemanager.admin.address", "10.8.2.26:8033");


        this.conf.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");

        this.conf.set("yarn.application.classpath", "/etc/hadoop/conf.cloudera.hdfs,"
                + "/etc/hadoop/conf.cloudera.yarn,"
                + "/opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/lib/hadoop*//*,"
                + "/opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/lib/hadoop/lib*//*,"
                + "/opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/lib/hadoop-hdfs*//*,"
                + "/opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/lib/hadoop-hdfs/lib*//*,"
                + "/opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/lib/hadoop-yarn*//*,"
                + "/opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/lib/hadoop-yarn/lib*//*");
         */

		fs = FileSystem.get(conf);
        //System.out.println("++++++++++++ MerraRecordReaderForDBIndex " + fs.getStatus().getUsed());

		//long sTime = System.currentTimeMillis();
		inputStream = fs.open(new Path(this.arrayInputSplit.getFilePath()));
		//long eTime = System.currentTimeMillis();
		//LOG.info("*******************************   Fs open file : " + (eTime - sTime)*1.0/1000.0 + " seconds");
		this.initializeElementNum(this.arrayInputSplit.getArraySpecList());
		this.hostname = getLocalhost();
	}
	
	public void initialize(InputSplit paramInputSplit, FileSystem fs) throws IOException,
    																		 InterruptedException {
		this.arrayInputSplit = (ArrayBasedSplitByDBIndex) paramInputSplit;
		this.fs = fs;
		//long sTime = System.currentTimeMillis();
		inputStream = fs.open(new Path(this.arrayInputSplit.getFilePath()));
		//long eTime = System.currentTimeMillis();
		//LOG.info("*******************************   Fs open file : " + (eTime - sTime)*1.0/1000.0 + " seconds");
		this.initializeElementNum(this.arrayInputSplit.getArraySpecList());
	}
	
	public void initializeElementNum(List<ArraySpec> arraySpecList ) {
		List<SpatioRange> spatioRngList = new ArrayList<SpatioRange>();
		for(ArraySpec arraySpec : arraySpecList) {
			spatioRngList.add(new SpatioRange(arraySpec.getCorner(), arraySpec.getShape()));
		}
		
		this.initializeElementNumBySpace(spatioRngList);		
	}
	
	public void initializeElementNumBySpace(List<SpatioRange> spatioRngList) {
		
		for(SpatioRange sptRng : spatioRngList) {
			int num = 1;
			for(int i=0; i<sptRng.shape.length; i++) {
				num = num * sptRng.shape[i];
			}
			
			totalElems += num;
		}
		
		elemPos = new int[totalElems];
		
		int count = 0;
		
		for(SpatioRange sptRng : spatioRngList) {
			int[] corner = sptRng.corner;
			int[] shape = sptRng.shape;
			 
			for(int i=corner[0]; i<corner[0]+shape[0]; i++) {
				for(int j=corner[1]; j<corner[1]+shape[1]; j++) {
					elemPos[count] = i*xSize + j;
					count++;
				}
			}
		}		
		
		//LOG.info("TotalElem number == " + totalElems + " count number == " + count);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		keyNum++;
		if(keyNum >= this.arrayInputSplit.getVarInfoList().size()) {
			this.conf.set("localData", this.localDataNum+"");
			this.conf.set("totalData", this.totalDataNum+"");
			return false;
		} else {
			return true;
		}
	}

	@Override
	public VariableInfo getCurrentKey() throws IOException, InterruptedException {
		return this.arrayInputSplit.getVarInfoList().get(keyNum);
	}

	@Override
	public Array getCurrentValue() throws IOException, InterruptedException {	
		long length = this.arrayInputSplit.getVarInfoList().get(keyNum).getLength();//113256;
		long offset = this.arrayInputSplit.getVarInfoList().get(keyNum).getOffset();	
		byte[] byteBuffer = new byte[(int) length];
		
		long sTime = System.currentTimeMillis();
		int n = this.inputStream.read(offset, byteBuffer, 0, (int) length);
		//DFSClient dfs = new DFSClient(conf);
		BlockLocation[] blockLocations = this.fs.getFileBlockLocations(new Path(this.arrayInputSplit.getFilePath()), (int) offset, (int) length);
		String[] hosts = blockLocations[0].getHosts();
		
		this.totalDataNum = this.totalDataNum + (int) length;
		if(this.hostname.equals(hosts[0])) {
			this.localDataNum = this.localDataNum + (int) length;
		}
		
		//LOG.info("*********** localhost is " + hostname + "****");
		//LOG.info("***********" + blockLocations.length + "********************  " + this.getCurrentKey().getShortName() + " time: " + this.getCurrentKey().getTime() + " read from " + Arrays.toString(hosts));
		
		//LocatedBlocks locatedBlocks = dfs.getLocatedBlocks(this.arrayInputSplit.getFilePath(), offset);		
		//System.out.println("++++++++++++++++++++++++++++++  " + locatedBlocks.get(0).getLocations()[0].getHostName());
		//long eTime = System.currentTimeMillis();
		//LOG.info("*******************************   Fs read values : " + (eTime - sTime)*1.0/1000.0 + " seconds");
		
		InputStream in = new ByteArrayInputStream(byteBuffer);
		InputStream zin = new java.util.zip.InflaterInputStream(in);
		ByteArrayOutputStream out = new ByteArrayOutputStream(361*540*4);
        IO.copy(zin, out);
        byte[] buffer = out.toByteArray();
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        FloatBuffer fltBuffer = bb.asFloatBuffer();
        float[] data = new float[this.totalElems];
        for(int i=0; i<this.totalElems; i++) {
        	data[i] = fltBuffer.get(this.elemPos[i]);
        	//System.out.println(this.arrayInputSplit.getVarInfoList().get(keyNum).getShortName() + " value" + i + " is " + data[i]);
        }
       
        int[] shape =new int[1];
        shape[0] = this.totalElems;
        return Array.factory(float.class, shape, data);
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return keyNum*1.0f / (this.arrayInputSplit.getVarInfoList().size()*1.0f);
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		inputStream.close();
		fs.close();
		LOG.info("*****close****** local data is " + this.localDataNum + " , total data is " + this.totalDataNum);	
	}
	
	public String getLocalhost() throws IOException {
		byte[] localhost = new byte[30];	
		Runtime.getRuntime().exec("hostname").getInputStream().read(localhost);
		String hostname = new String(localhost);
		hostname = hostname.split("\n")[0];
		return hostname;
	}
	
	public int getTotalDataNum() {
		return this.totalDataNum;
	}
	
	public int getLocalDataNum() {
		return this.localDataNum;
	}

}
