package edu.gmu.stc.mapreduce.map;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.gmu.stc.mapreduce.io.ArraySpec;
import edu.gmu.stc.mapreduce.io.SumNum;
import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import ucar.ma2.IndexIterator;


public class AverageFloatSimpleMapper extends Mapper<ArraySpec, Array, Text, SumNum>{
	private static final Log LOG = LogFactory.getLog(AverageFloatSimpleMapper.class);
	private Text keyOutput = new Text();
	SumNum valueOutputSumNum = new SumNum();
	long sTime, eTime, ioTime = 0, mapTime = 0, count = 0;
	
	public void map(ArraySpec key, Array value, Context context) throws IOException, InterruptedException {
		
		ArrayFloat ncArray = (ArrayFloat) value;
		IndexIterator itor = ncArray.getIndexIterator();
		Float vmax = key.getVmax();//1.0E30f;
	    Float vmin = key.getVmin();//-1.0E30f;
	    Float fillValue = key.getFillValue();//9.9999999E14f;
		Float sum = 0F;
		int num = 0;
		//int count = 0;
		long timerB = System.currentTimeMillis();
		
		while(itor.hasNext()) {
			count++;
			Float tempValue = itor.getFloatNext();
			if(tempValue>vmax || tempValue<vmin || tempValue.equals(fillValue)) {
            	continue;
            } else {
            	sum = tempValue + sum;
            	num++;
            }		
		}
		
		long timerC = System.currentTimeMillis();
	    mapTime = mapTime + timerC - timerB;
	    
	    try {
	    	  valueOutputSumNum.setSum(sum);
      	  	  valueOutputSumNum.setNum(num);
			  //SumNum valueOutputSumNum = new SumNum(sum, num);
			  String name = key.getFileName();
			  String[] fileName = key.getFileName().toString().split("\\.");
			  String date = fileName[fileName.length-2];
			  keyOutput.set( key.getVarName() + "  " + date);					  
			  context.write(keyOutput, valueOutputSumNum);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
	    
	    //LOG.info("Key =  " + keyOutput + key.getVarName() + ",    value = " + valueOutputSumNum );
	}
	
	public void run(Context context) throws IOException, InterruptedException
	 {		
		 setup(context);
		 sTime = System.currentTimeMillis();
		 try {
			     while (context.nextKeyValue()) {
			        
			        	
			        map(context.getCurrentKey(), context.getCurrentValue(), context);
			        	
			     }
			  } finally {
				  /*float localData = Float.parseFloat(context.getConfiguration().get("localData"));
				  float totalData = Float.parseFloat(context.getConfiguration().get("totalData"));
				  
				  Text localDataKey = new Text("localData");
				  SumNum localDataValue = new SumNum(localData, 1);
				  context.write(localDataKey, localDataValue);
				  
				  Text totalDataKey = new Text("totalData");
				  SumNum totalDataValue = new SumNum(totalData, 1);
				  context.write(totalDataKey, totalDataValue);
			      cleanup(context);*/
			     }
	 }
	

	protected void cleanup(Context context) throws IOException, InterruptedException
	 {
		/*eTime = System.currentTimeMillis();
		ioTime = ioTime + eTime - sTime - this.mapTime;
		LOG.info("*******************************   FS-io read " + this.count + " values : " + this.ioTime*1.0/1000.0 + " seconds;"
				+ " speed:  " + this.count*4.0/1024/1024/(this.ioTime/1000) + "MB/S");
		LOG.info("*******************************   Mapper takes =====" + mapTime*1.0/1000.0 + " seconds");*/
	 }
}
