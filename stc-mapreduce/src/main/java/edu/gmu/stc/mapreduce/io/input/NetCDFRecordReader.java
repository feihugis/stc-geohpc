package edu.gmu.stc.mapreduce.io.input;

import org.apache.hadoop.mapreduce.RecordReader;

/**
 * NetCDF specific code for reading data from NetCDF files.
 * This class is used by Map tasks to read the data assigned to them
 * from NetCDF files. 
 * TODO: don't copy data out of an InputSplit and the store it internally.
 * Rather, keep the split around and just access the data as needed
 * 
 * Original author is edu.ucsc.srl.damasc
 * adapted by Fei Hu
 */




import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.gmu.stc.mapreduce.Utils;
import edu.gmu.stc.mapreduce.io.ArraySpec;
import edu.gmu.stc.mapreduce.io.NcHdfsRaf;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.ma2.Array;
import ucar.nc2.Dimension;
import ucar.ma2.InvalidRangeException;

public class NetCDFRecordReader extends RecordReader<ArraySpec, Array> {

  private static final Log LOG = LogFactory.getLog(NetCDFRecordReader.class);
  private long _timer;
  private int _numArraySpecs;

  //this will cause the library to use its default size
  private int _bufferSize = -1; 

  private NetcdfFile _ncfile = null;
  private NcHdfsRaf _raf = null;
  private Variable _curVar; // actual Variable object
  private String _curVarName; // name of the current variable that is open
  private String _curFileName;
  private Configuration conf;
  // how many data elements were read the last step 
  private long _totalDataElements = 1; 

  // how many data elements have been read so far (used to track work done)
  private long _elementsSeenSoFar = 0; 

  private ArrayList<ArraySpec> _arraySpecArrayList = null;

  private ArraySpec _currentArraySpec = null; // this also serves as key
  private Array _value = null;
  private int _currentArraySpecIndex = 0;

  /**
   * Resets a RecordReader each time it is passed a new InputSplit to read
   * @param genericSplit an InputSplit (really an ArrayBasedFileSplit) that
   * needs its data read
   * @param context TaskAttemptContext for the currently executing progrma
  */
  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) 
                         throws IOException {
      
    this._timer = System.currentTimeMillis();
    ArrayBasedFileSplit split = (ArrayBasedFileSplit) genericSplit;
    this._numArraySpecs = split.getArraySpecList().size();
    this.conf = context.getConfiguration();

    Path path = split.getPath();
    FileSystem fs = path.getFileSystem(this.conf);
        
    this._arraySpecArrayList = split.getArraySpecList();

    // calculate the total data elements in this split
    this._totalDataElements = 0;

    for ( int j=0; j < this._arraySpecArrayList.size(); j++) {
      this._totalDataElements += this._arraySpecArrayList.get(j).getSize();
    }

    // get the buffer size
    this._bufferSize = Utils.getBufferSize(this.conf);
    
    this._raf = new NcHdfsRaf(fs.getFileStatus(path), this.conf, this._bufferSize);
    this._ncfile = NetcdfFile.open(this._raf, path.toString());
           
  }

  /** this is called to load the next key/value in. The actual data is retrieved
   * via getCurrent[Key|Value] calls
   * @return a boolean that is true if there is more data to be read, 
   * false otherwise
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if ( !this._arraySpecArrayList.isEmpty() ) {
      // set the current element
      this._currentArraySpec = this._arraySpecArrayList.get(0);
      
      // then delete it from the ArrayList
      this._arraySpecArrayList.remove(0);

      // fixing an entirely random bug -jbuck TODO FIXME
      /*if ( this._currentArraySpec.getCorner().length <= 1 ) {
        return this.nextKeyValue();
      }*/

      // transfer the data
      loadDataFromFile(); 

      return true;
    } else {
      this._timer = System.currentTimeMillis() - this._timer;
      LOG.debug("from init() to nextKeyValue() returning false, " +
                "this record reader took: " + this._timer + 
                " ms. It had " + this._numArraySpecs + 
                " ArraySpecs to process" );
      
      this.conf.set("localData", this._raf.getLocalDataNum()+"");
      this.conf.set("totalData", this._raf.getTotalDataNum()+"");
      LOG.info("************************ Local Data Size is " + this._raf.getLocalDataNum() 
    		  						+ "Total Data Size is " + this._raf.getTotalDataNum()
    		  						+ "Ratio is " + this._raf.getLocalDataNum()/this._raf.getTotalDataNum());
      return false;
    }
  }

  /**
   * Load data into the value element from disk.
   * Currently this only supports IntWritable. Extend this 
   * to support other data types TODO
   */
  private void loadDataFromFile() throws IOException {
    try { 

      // reuse the open variable if it's the correct one
      if ( this._curVarName == null || 0 != (this._currentArraySpec.getVarName()).compareTo(this._curVarName)){
        LOG.debug("calling getVar on " + this._currentArraySpec.getVarName() );
        this._curVar = this._ncfile.findVariable(this._currentArraySpec.getVarName());
      }
            

      if ( this._curVar ==null ) {
        LOG.warn("this._curVar is null. BAD NEWS");
        LOG.warn( "file: " + this._currentArraySpec.getFileName() + 
            "corner: " +   
            Arrays.toString(this._currentArraySpec.getCorner() ) + 
            " shape: " + Arrays.toString(this._currentArraySpec.getShape() ) );
      }

      /*LOG.warn( " File: " + this._currentArraySpec.getFileName() + 
                " startOffset: " + Utils.arrayToString(this._currentArraySpec.getLogicalStartOffset()) + 
               "corner: " + 
               Arrays.toString(this._currentArraySpec.getCorner()) + 
               " shape: " + 
               Arrays.toString(this._currentArraySpec.getShape()));*/

      // this next bit is to be able to set the dimensions of the variable
      // for this ArraySpec. Needed for flattening the groupID to a long
      ArrayList<Dimension> varDims = new ArrayList<Dimension>(this._curVar.getDimensions());
      int[] varDimLengths = new int[varDims.size()];

      for( int i=0; i<varDims.size(); i++) {
        varDimLengths[i] = varDims.get(i).getLength();
      }
                
      this._currentArraySpec.setVariableShape(varDimLengths);
      this._currentArraySpec.setVarName(this._curVar.getShortName());
            

      if(   this._curVar.getShortName().equals("iobs_res_1")
         || this._curVar.getShortName().equals("num_observations_500m")
         || this._curVar.getShortName().equals("obscov_500m_1")
         || this._curVar.getShortName().equals("q_scan_1")
         || this._curVar.getShortName().equals("QC_500m_1")
         || this._curVar.getShortName().equals("sur_refl_b01_1")
         || this._curVar.getShortName().equals("sur_refl_b02_1")
         || this._curVar.getShortName().equals("sur_refl_b03_1")
         || this._curVar.getShortName().equals("sur_refl_b04_1")
         || this._curVar.getShortName().equals("sur_refl_b05_1")
         || this._curVar.getShortName().equals("sur_refl_b06_1")
         || this._curVar.getShortName().equals("sur_refl_b07_1")) {
        int[] crn = new int[]{0,0};
        int[] shp = new int[]{2400,2400};
        long t_start = System.currentTimeMillis();
        this._value = this._curVar.read( crn, shp);
        long t_end = System.currentTimeMillis();
        System.out.println("----- " + this._curVar.getShortName() + " ---------" + " Read variables : " + (t_end - t_start));
        //this._value = this._curVar.read();
      } else {
        this._value = this._curVar.read( this._currentArraySpec.getCorner(),
                                         this._currentArraySpec.getShape()
        );
      }



    } catch (InvalidRangeException ire) {
    // convert the InvalidRangeException (netcdf specific) 
    // to an IOException (more general)
      throw new IOException("InvalidRangeException caught in " + 
                            "NetCDFRecordReader.loadDataFromFile()" + 
                            "corner: " + 
                           Arrays.toString(this._currentArraySpec.getCorner()) +
                            " shape: " + 
                            Arrays.toString(this._currentArraySpec.getShape()));
    }
  }
  
  /**  
   * Update _elementsSeenSoFar so that getProgress() is correct(-ish).
   * This is used to track task (and job) progress.
   */
  private void updateProgress() {
    this._elementsSeenSoFar += this._currentArraySpec.getSize();
  }

  /**
   * Returns the current key
   * @return An ArraySpec object indicating the current key
   */
  @Override
  public ArraySpec getCurrentKey() {
    // updates the counter for work done so far
    updateProgress();

    // extract the cell coordinate pointed to by the current key
    
    return this._currentArraySpec;
  }

  /**
   * return the current Value, in the Key/Value sense.
   * @return an Array object containing the values that correspond 
   * to the ArraySpec that is the current Key
   */
  @Override
  public Array getCurrentValue() {
	  
	 //LOG.info("\t\t**********value************");
	 return this._value;
  }

  /**
   * Returns this jobs progress to the JobTracker so that the GUIs
   * can be updated.
   * @return a float representing the percent of this job that is
   * compeleted (between 0 and 1)
   */
  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (float)(this._elementsSeenSoFar / this._totalDataElements);
  }

  /**
   * Close files that were opened by the Record Reader and do general cleanup
   */
  @Override
  public void close() throws IOException {
    try {
      if (this._ncfile != null) {
        this._ncfile.close();
      }
    } catch (IOException ioe) {
      LOG.warn("ioe thrown in NetCDFRecordReader.close()\n");
    }
  }
}

