package edu.gmu.stc.hadoop.index.combine;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.gmu.stc.database.IndexOperator;
import edu.gmu.stc.hadoop.index.io.merra.VariableInfo;


public class BuildIndexCombiner extends Reducer<Text, VariableInfo, Text, VariableInfo>{
	private static final Log LOG = LogFactory.getLog(BuildIndexCombiner.class);
	private IndexOperator idxOptor = new IndexOperator();
	
	public void reduce(Text key, Iterable<VariableInfo> value, Context context) throws IOException, InterruptedException {
		float sum = 0.0f;
		List<VariableInfo> varInfoList = new ArrayList<VariableInfo>();
		
		Iterator<VariableInfo> itor = value.iterator();
		while(itor.hasNext()) {
			VariableInfo v = (VariableInfo) itor.next();
			VariableInfo addv = new VariableInfo(v.getShortName(), v.getTime(), v.getOffset(), v.getLength(), v.getBlockHosts(), v.getComp_Code());
			addv.setDateType(v.getDateType());
			varInfoList.add(addv);
			sum = sum + 1;
		}
		
		long time0 = System.currentTimeMillis();
		idxOptor.addVarRecordList(varInfoList, varInfoList.get(0).getDateType());
		long time1 = System.currentTimeMillis();
		LOG.info("++++++ MS : " + (time1 - time0));
		
		VariableInfo varInfo = new VariableInfo();
		varInfo.setDateType(sum+"");
		context.write(key, varInfo);
		
	}
}
