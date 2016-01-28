package edu.gmu.stc.hadoop.index.io.merra;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * This class represents a generic array. It stores 
 * the data required to open a file and read a contigous 
 * array shape from a variable in said file.
 * 
 * Original author is edu.ucsc.srl.damasc
 * adapted by Fei Hu
 */

public class ArraySpec implements WritableComparable {
  private String _fileName = "";
  private String _varName = "";
  private int[] _shape = new int[3];
  private int[] _corner = new int[3];  // anchor point
  private int[] _varShape = new int[3]; // shape of the entire variabe
  private int[] _logicalStartOffset = new int[3]; // used to adjust the coordinates
                                    // of the input (in logical space)
  private Float _vmax = 1.0E30f;
  private Float _vmin = -1.0E30f;
  private Float _fillValue = 9.9999999E14f;
  
  private int _bboxType = 0;
  private long offset = 0;
  private long length = 0;

  public ArraySpec() {}
  
  
  
  /**
   * Constructor
   * @param corner The n-dimensional coordinate for the anchoring
   * corner of the array to be read
   * @param shape The n-dimension shape of the data to be read, 
   * starting at corner
   * @param varName Name of the variable to read the array from 
   * @param fileName Name of the file to open for reading
   * @param variableShape The shape of the variable containing
   * this ArraySpec
   */  
  public ArraySpec(int[] corner, int[] shape, 
                   String varName, String fileName,
                   int[] variableShape) 
                   throws Exception {
  
    if ( shape.length != corner.length ) {
      throw new Exception ("shape and length need to be of the same length");
    }

    this._shape = new int[shape.length];
    for (int i=0; i < shape.length; i++) {
      this._shape[i] = shape[i];
    }

    this._corner = new int[corner.length];
    for (int i=0; i < corner.length; i++) {
      this._corner[i] = corner[i];
    }

    this._varShape = new int[variableShape.length];
    for( int i=0; i< variableShape.length; i++) {
      this._varShape[i] = variableShape[i];
    }

    _varName = new String(varName);
    _fileName = new String(fileName);
  }

  /**
   * Constructor where the variable shape is not known
   * @param corner The n-dimensional coordinate for the anchoring
   * corner of the array to be read
   * @param shape The n-dimension shape of the data to be read, 
   * starting at corner
   * @param varName Name of the variable to read the array from 
   * @param fileName Name of the file to open for reading
   */
  public ArraySpec(int[] corner, int[] shape, 
                   String varName, String fileName) throws Exception {
    this( corner, shape, varName, fileName, new int[0]);
  }
  
  public ArraySpec(int[] corner, int[] shape, String varName) throws Exception {

	if ( shape.length != corner.length ) {
	throw new Exception ("shape and length need to be of the same length");
	}
	
	this._shape = new int[shape.length];
	for (int i=0; i < shape.length; i++) {
	this._shape[i] = shape[i];
	}
	
	this._corner = new int[corner.length];
	for (int i=0; i < corner.length; i++) {
	this._corner[i] = corner[i];
	}
		
	_varName = new String(varName);
  }
  
  //this is for constructed by SpatioRange
   public ArraySpec(int[] corner, int[] shape, float vmax, float vmin, float fillValue) {
	  this._shape = new int[shape.length];
		for (int i=0; i < shape.length; i++) {
		this._shape[i] = shape[i];
		}
		
		this._corner = new int[corner.length];
		for (int i=0; i < corner.length; i++) {
		this._corner[i] = corner[i];
		}
		
		this._vmax = vmax;
		this._vmin = vmin;
		this._fillValue = fillValue;
   }

  /** 
   * return the number of dimensions for both shape and corner
   * @return the number of dimensions for variable, corner and shape
   * (note: one value is returned, all three must have the same number
   * of dimensions)
   */
  public int getRank() {
    return this._shape.length;
  }

  /**
   * Return the corner that anchors the array represented by this ArraySpec
   * @return an array of integers representing the coordinate of the corner
   * in the respective dimension (array index zero has the coordinate for the 
   * zero-th dimension, etc.)
   */
  public int[] getCorner() {
    return this._corner;
  }

  /**
   * Return the shape to be read from the array represented by this ArraySpec
   * @return an array of integers representing the length of the shape 
   * for the respective dimension (array index zero has the length of the 
   * zero-th dimension, etc.)
   */
  public int[] getShape() {
    return this._shape;
  }

  /**
   * Return the shape of the n-dimensional variable that contains the 
   * array represented by this ArraySpec.
   * @return an n-dimension array of integers storing the length of 
   * the variable in the corresponding array location
   */
  public int[] getVariableShape() {
    return this._varShape;
  }

  /**
   * Get the logical offset for this ArraySpec. This is used to place
   * ArraySpecs in logical spaces spanning multiple files where as 
   * shape and corner and always relative to the specific variable (in the 
   * specific file) being read.
   * @return an n-dimensional array representing the location of this 
   * ArraySpec in the logical space of the currently executing query
   */
  public int[] getLogicalStartOffset() {
    return this._logicalStartOffset;
  }

  /**
   * Return the name of the variable containing the data represented by 
   * this ArraySpec
   * @return name of the Variable containing this ArraySpec
   */
  public String getVarName() {
    return _varName;
  }

  public void setVarName(String varName) {
	  this._varName = varName;
  }
  /**
   * Return the name of the file containing the variable which holds
   * the data represented by this ArraySpec.
   * @return the file name that corresponds to this ArraySpec 
   */
  public String getFileName() {
    return _fileName;
  }

  /**  
   * Get the number of cells represented by this ArraySpec.
   * @return number of cells represented by this ArraySpec.
   */
  public long getSize() {
    long size = 1;
    for (int i = 0; i < this._shape.length; i++) {
      size *= this._shape[i];
    }

    return size;
  }

  /**
   * Set the shape of the data to be read
   * @param newShape shape of the data to be read
   */
  public void setShape( int[] newShape ) {
    // might want to do some checking of old shape vs new shape later
    this._shape = newShape;
  }
  
  public void setOffset(long offset) {
	  this.offset = offset;
  }
  
  public long getOffset() {
	  return this.offset;
  }
  
  public void setLength(long length) {
	  this.length = length;
  }
  
  public long getLength() {
	  return this.length;
  }

  /**
   * Set the shape of the variable that contains the data represented
   * by this ArraySpec.
   * @param newVarShape the Shape of the variable that contains the
   * data for this ArraySpec
   */ 
  public void setVariableShape( int[] newVarShape) {
    this._varShape = newVarShape;
  }

  /**
   * Sets the logical offset of the this ArraySpec
   * @param newLogicalStartOffset the offset, in the global logical
   * space, where this ArraySpec resides
   */
  public void setLogicalStartOffset( int[] newLogicalStartOffset ){
    this._logicalStartOffset = newLogicalStartOffset;
  }

  /**
   * Write the contents of this ArraySpec out to a string
   * @return a String representation of this object
   */
  public String toString() {
    return _fileName + ": var: " + _varName + ": corner = " + 
           Arrays.toString(_corner) +
           ", shape = " + Arrays.toString(_shape); 
  }

  /**
   * Compares the current ArraySpec to another ArraySpec
   * @return an integer that is less than, equal to, or greater than
   * zero depending on whether the object passed in is less than,
   * equal to or greater than this object, respectively
   */
  public int compareTo(Object o) {
    int retVal = 0;
    ArraySpec other = (ArraySpec)o;

    if ( 0 != this._fileName.compareTo(other.getFileName())){
      return this._fileName.compareTo(other.getFileName());
    }

    if ( 0 != this._varName.compareTo(other.getVarName())){
      return this._varName.compareTo(other.getVarName());
    }

    for ( int i = 0; i < this._corner.length; i++) {
      retVal = this._corner[i] - other.getCorner()[i];

      if (retVal != 0) {
        return retVal;
      }
    }

    return retVal;
  }

  /**
   * Serialize the contents of this ArraySpec to a DataOutput object
   * @param out The DataOutput object to write the contents of this 
   * ArraySpec to
   */
  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, _fileName);
    Text.writeString(out, _varName);

    out.writeInt(_shape.length);
    for (int i = 0; i < _shape.length; i++)
      out.writeInt(_shape[i]);

      out.writeInt(_corner.length);
      for (int i = 0; i < _corner.length; i++)
        out.writeInt(_corner[i]);

      out.writeInt(_varShape.length);
      for (int i = 0; i < _varShape.length; i++)
        out.writeInt(_varShape[i]);

      if ( null == _logicalStartOffset ) {
        out.writeInt(0);
      } else  {
        out.writeInt(_logicalStartOffset.length);
        for (int i= 0; i < _logicalStartOffset.length; i++) {
          out.writeInt(_logicalStartOffset[i]);
        }
      }
      
      out.writeFloat(this._vmax);
      out.writeFloat(this._vmin);
      out.writeFloat(this._fillValue);
      out.writeInt(this._bboxType);
      out.writeLong(this.offset);
      out.writeLong(this.length);
  }

  /**
   * Populate an ArraySpec object by reading data from a 
   * DataInput object
   * @param in The DataInput object to read the data from
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    _fileName = Text.readString(in);
    _varName = Text.readString(in);
       
    int len = in.readInt();
    _shape = new int[len];
    for (int i = 0; i < _shape.length; i++)
      _shape[i] = in.readInt();
        
    len = in.readInt();
    _corner = new int[len];
    for (int i = 0; i < _corner.length; i++)
      _corner[i] = in.readInt();

    len = in.readInt();
    _varShape = new int[len];
    for (int i = 0; i < _varShape.length; i++)
      _varShape[i] = in.readInt();

    len = in.readInt();
    if ( 0 == len )  {
      _logicalStartOffset = null;
    } else { 
      _logicalStartOffset = new int[len];
      for (int i = 0; i < _logicalStartOffset.length; i++)
        _logicalStartOffset[i] = in.readInt();
    }
    
    this._vmax = in.readFloat();
    this._vmin = in.readFloat();
    this._fillValue = in.readFloat();
    this._bboxType = in.readInt();
    this.offset = in.readLong();
    this.length = in.readLong();
    
  }
  
  public void setVmax( Float vmax) {
	  this._vmax = vmax;
  }
  
  public void setVmin( Float vmin) {
	  this._vmin = vmin;
  }
  
  public void setFillValue( Float fillValue) {
	  this._fillValue = fillValue;
  }
  
  public Float getVmax() {
	  return this._vmax;
  }
  
  public Float getVmin() {
	  return this._vmin;
  }
  
  public Float getFillValue() {
	  return this._fillValue;
  }
  
  public void setBboxType(int type) {
	  this._bboxType = type;
  }
  
  public int getBboxType() {
	  return this._bboxType;
  }
  
  
}

