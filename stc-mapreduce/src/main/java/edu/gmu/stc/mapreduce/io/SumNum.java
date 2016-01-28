package edu.gmu.stc.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//import org.apache.commons.math.stat.descriptive.summary.Sum;
import org.apache.hadoop.io.Writable;

/**
 * @author feihu
 * 
 * This is for AverageFloat to transfer sum and num data among mapper, combiner and reducer.
 *
 */
public class SumNum implements Writable{
	private String _varName;
	private Float _sum;
	private int _num;
	
	/**
	 * Constructor
	 * @param varName
	 * @param sum
	 * @param num
	 */
	public SumNum( String varName, Float sum, int num) {
		this._varName = varName;
		this._sum = sum;
		this._num = num;
	}
	
	/**
	 * Constructor
	 * @param sum
	 * @param num
	 */
	public SumNum( Float sum, int num) {
		this._sum = sum;
		this._num = num;
	}
	
	public SumNum() {
		
	}
	
	/* return a string about the attributes of SumNum
	 * 
	 */
	@Override
	public String toString() {
		return this._varName + this._sum + this._num;
	}
	
	/* 
	 * to fix
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 
	@Override
	public int compareTo(Object o) {
		return -1;
	}*/
	
	/* 
	 * write function
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException{
		out.writeFloat(this._sum);
		out.writeInt(this._num);	
	}
	
	/* 
	 * read Files function
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException{
		this._sum = in.readFloat();
		this._num = in.readInt();
	}
	
	public static SumNum read(DataInput in) throws IOException {
		SumNum sm = new SumNum();
		sm.readFields(in);
		return sm;
	}
	
	/**
	 * @return
	 */
	public Float getSum() {
		return this._sum;
	}
	
	public int getNum() {
		return this._num;
	}
	
	public void setSum(Float sum) {
		this._sum = sum;
	}
	
	public void setNum(int num) {
		this._num = num;
	}
}

