package edu.gmu.stc.hadoop.index.io.merra;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ArraySpecList implements WritableComparable{
	List<ArraySpec> arraySpecList = new ArrayList<ArraySpec>();
	
	public ArraySpecList() {
		
	}
	
	public ArraySpecList(List<ArraySpec> input){
		this.arraySpecList = new ArrayList<ArraySpec>();
		for(ArraySpec arraySpec : input) {
			ArraySpec one;
			try {
				one = new ArraySpec(arraySpec.getCorner(), arraySpec.getShape(), arraySpec.getVarName());
				this.arraySpecList.add(one);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public List<ArraySpec> getArraySpecList() {
		return this.arraySpecList;
	}
	
	public void setArraySpecList(List<ArraySpec> input) {
		this.arraySpecList = new ArrayList<ArraySpec>(input);
	}	

	@Override
	public void write(DataOutput out) throws IOException {
		int size = this.arraySpecList.size();
		out.writeInt(size);
		for(int i=0; i<size; i++) {
			this.arraySpecList.get(i).write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.arraySpecList = new ArrayList<ArraySpec>();
		
		int size = in.readInt();
		for(int i=0; i<size; i++) {
			ArraySpec tmp = new ArraySpec();
			tmp.readFields(in);
			this.arraySpecList.add(tmp);
		}	
	}

	@Override
	public int compareTo(Object o) {
		int isSame = -1;
		
		ArraySpecList other = (ArraySpecList) o;
		for(ArraySpec one : this.arraySpecList) {
			
			for(ArraySpec target : other.getArraySpecList()) {
				isSame = one.compareTo(target);
				if(isSame == 0) {
					break;
				}
			}
			
			if(isSame != 0) {
				return isSame;
			}
		}
		
		return isSame;
	}

}
