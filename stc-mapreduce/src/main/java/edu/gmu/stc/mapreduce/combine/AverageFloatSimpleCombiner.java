package edu.gmu.stc.mapreduce.combine;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.gmu.stc.mapreduce.io.SumNum;


/**
 * @author feihu
 * 
 * combine all the values with the same varName
 *
 */
public class AverageFloatSimpleCombiner extends Reducer<Text, SumNum, Text, SumNum>{
	private SumNum outputValue = new SumNum();

	/* combine function
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
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
		
		outputValue.setSum(sum);
		outputValue.setNum(num);
		
		context.write(key, new SumNum( sum, num));		
	}

}
