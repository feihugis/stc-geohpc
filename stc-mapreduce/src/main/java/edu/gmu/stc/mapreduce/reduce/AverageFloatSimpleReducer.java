package edu.gmu.stc.mapreduce.reduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.gmu.stc.mapreduce.io.SumNum;

/**
 * reduce function for AverageFloat
 * @author feihu
 *
 */
public class AverageFloatSimpleReducer extends Reducer<Text, SumNum, Text, FloatWritable> {
	private static final Log LOG = LogFactory.getLog(AverageFloatSimpleReducer.class);
	private FloatWritable average = new FloatWritable();
	private Text outputKey = new Text();
	private float localData = 0.0f;
	private float totalData = 0.0f;
	
	/* reduce to get the average for a variable
	 * @param key
	 * @param value
	 * @param context
	 */
	public void reduce(Text key, Iterable<SumNum> value, Context context) throws IOException, InterruptedException {
			
		Iterator<SumNum> sumNums = value.iterator();
		int num = 0;
		Float sum = 0.0F;
		
		while(sumNums.hasNext()){
			SumNum sumNum = sumNums.next();
			sum = sum + sumNum.getSum();
			num = num + sumNum.getNum();
		}
		
		if(key.toString().equals("localData")) {
			this.localData = this.localData + sum.floatValue();
			return;
		}
		
		if(key.toString().equals("totalData")) {
			this.totalData = this.totalData + sum.floatValue();
			return;
		}
				
		average.set(sum/num);
		String[] name = key.toString().split("/");
		outputKey.set(name[name.length-1]+ "  mean:");
		context.write(outputKey, average);	
		//LOG.info("--------------- Key  ==  " + outputKey + " value  ==  " + average);
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		LOG.info("++++++++++++++++++++++++ Local Data Size is " + this.localData 
										 + ", TotalData Size is " + this.totalData 
										 + ", Ratio is " + this.localData / this.totalData);
	}

}
