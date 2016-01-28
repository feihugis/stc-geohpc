package edu.gmu.stc.hadoop.index.reduce;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

import edu.gmu.stc.hadoop.index.io.merra.VariableInfo;

public class BuildIndexReducer extends Reducer<Text, VariableInfo, Text, FloatWritable>{
	
	public void reduce(Text key, Iterable<VariableInfo> value, Context context) {
		float sum = 0.0f;
		Iterator<VariableInfo> itor = value.iterator();
		while(itor.hasNext()) {
			VariableInfo v = itor.next();
			sum = sum + Float.parseFloat(v.getDateType());
		}
		
		try {
			context.write(key, new FloatWritable(sum));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
