package edu.gmu.stc.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author feihu
 *Information for the variable in MERRA data
 */
public class VariableInfo implements WritableComparable{
	private String _shortName = "";
	private String _fullName = "";
	private float _vmax = 1.0E30f;
	private float _vmin = -1.0E30f;
	private float _fillValue= 9.9999999E14f;;
	
	private int _time = 0;
	private long _offset = 0;
	private long _length = 0;
	private String _blockHosts = "";
 
	private int _comp_Code;		/*
									COMP_CODE_NONE = 0;    // don't encode at all, just store
									COMP_CODE_RLE = 1;     // for simple RLE encoding
									COMP_CODE_NBIT = 2;    // for N-bit encoding
									COMP_CODE_SKPHUFF = 3; // for Skipping huffman encoding
									COMP_CODE_DEFLATE = 4; // for gzip 'deflate' e
								*/
	private String _dateType = "";
	
	public VariableInfo() {
		
	}
	
	public VariableInfo(String shortName, int time, long offset, long length, String blockHosts, int comp_Code) {
		this._shortName = shortName;
		this._time = time;
		this._offset = offset;
		this._length = length;
		this._blockHosts = blockHosts;
		this._comp_Code = comp_Code;
	}
	
	public VariableInfo(String shortName, String fullName, float vmax, float vmin, float fillValue) {
		this._shortName = shortName;
		this._fullName = fullName;
		this._vmax = vmax;
		this._vmin = vmin;
		this._fillValue = fillValue;
	}
	
	public String getShortName() {
		return this._shortName;
	}
	
	public String getFullName() {
		return this._fullName;
	}
	
	public float getVmax() {
		return this._vmax;
	}
	
	public float getVmin() {
		return this._vmin;
	}
	
	public float getFillValue() {
		return this._fillValue;
	}
	
	public int getTime() {
		return this._time;
	}
	
	public long getOffset(){
		return this._offset;
	}
	
	public long getLength() {
		return this._length;
	}
	
	public String getBlockHosts() {
		return this._blockHosts;
	}
	
	public int getComp_Code() {
		return this._comp_Code;
	}
	
	public void setDateType(String dateType) {
		this._dateType = new String(dateType);
	}
	
	public String getDateType() {
		return this._dateType;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, this._shortName);
		Text.writeString(out, this._fullName);
		out.writeFloat(this._vmax);
		out.writeFloat(this._vmin);
		out.writeFloat(this._fillValue);
		out.writeInt(this._time);
		out.writeLong(this._offset);
		out.writeLong(this._length);
		Text.writeString(out, this._blockHosts);
		out.writeInt(this._comp_Code);		
		Text.writeString(out, this._dateType);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this._shortName = new String(Text.readString(in));
		this._fullName = new String(Text.readString(in));
		this._vmax = in.readFloat();
		this._vmin = in.readFloat();
		this._fillValue = in.readFloat();
		this._time = in.readInt();
		this._offset = in.readLong();
		this._length = in.readLong();
		this._blockHosts = new String(Text.readString(in));
		this._comp_Code = in.readInt();
		this._dateType = new String(Text.readString(in));
	}

	@Override
	public int compareTo(Object objt) {
		// TODO Auto-generated method stub
		VariableInfo varInfo = (VariableInfo) objt;
		if( !this._blockHosts.equals(varInfo._blockHosts)) {
			return 1;
		} else {
			if( !this._shortName.equals(varInfo.getShortName())) {
				return 1;
			} else {
				if( this._time != varInfo.getTime()) {
					return 1;
				} else {
					if( this._offset != varInfo.getOffset()) {
						return 1;
					} else {
						if( this._length != varInfo.getLength()) {
							return 1;
						} else {
							return 0;
						}
					}
				}
			}		
		}
		
	}

	
}
