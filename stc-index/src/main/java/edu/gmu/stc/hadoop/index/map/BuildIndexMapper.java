package edu.gmu.stc.hadoop.index.map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

import edu.gmu.stc.database.IndexOperator;
import edu.gmu.stc.hadoop.index.io.merra.VariableInfo;


public class BuildIndexMapper extends Mapper<Text,Text,Text,VariableInfo>{
	private static final Log LOG = LogFactory.getLog(BuildIndexMapper.class);
	private IndexOperator idxOptor = new IndexOperator();
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String file = key.toString();
		String[] tmp = value.toString().split(",");
		String[] variables = new String[tmp.length];
		for(int i=0; i<variables.length; i++) {
			variables[i] = tmp[i];
		}
		
		long time0 = System.currentTimeMillis();
		List<VariableInfo> varInfoList = idxOptor.getVariableInfo(file, variables);
		long time1 = System.currentTimeMillis();
		LOG.info("++++++ MS : " + (time1 - time0));
		for(VariableInfo varInfo : varInfoList) {
			context.write(new Text(varInfo.getShortName()+"_" + varInfo.getDateType()), varInfo);
		}
	}
}
