package edu.gmu.stc.hadoop.raster.hdf5;

import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import edu.gmu.stc.hadoop.raster.ChunkFactory;
import edu.gmu.stc.hadoop.raster.DataChunk;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

/**
 * Created by Fei Hu on 2/17/16.
 */
public class H5Chunk extends DataChunk {
  String varShortName;
  String filePath;

  public H5Chunk(String shortName, String path, int[] corner, int[] shape, String[] dimensions, long filePos, long byteSize,
                 int filterMask, String[] hosts, String dataType) {
    super(corner, shape, dimensions, filePos, byteSize, filterMask, hosts, dataType);
    varShortName = shortName;
    filePath = path;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    Text.writeString(out, varShortName);
    Text.writeString(out, filePath);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    varShortName = Text.readString(in);
    filePath = Text.readString(in);
  }

  public String getVarShortName() {
    return varShortName;
  }

  public void setVarShortName(String varShortName) {
    this.varShortName = varShortName;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public String toString() {
    String output = varShortName + " corner : ";
    for (int corner : getCorner()) {
      output = output + corner + " ";
    }
    output += "start : " + getFilePos() + " end : " + (getFilePos() + getByteSize());
    return output;
  }

  public static void checkChunk(Variable variable, NetcdfFile ncfile) throws IOException, InvalidRangeException {
    List<DataChunk> chunkList = ChunkFactory.geneDataChunks(variable, ncfile.getFileTypeId(), ncfile.getLocation());
    int count = 0;
    long min = Long.MAX_VALUE;
    long max = 0;
    long size = 0;
    for (int i=0; i<chunkList.size()-1; i++) {
      count++;
      long start = chunkList.get(i).getFilePos();
      long end = chunkList.get(i).getFilePos() + chunkList.get(i).getByteSize();

      if (min > start) min = start;
      if (end > max) max = end;
      size = size + chunkList.get(i).getByteSize();

      if(end - chunkList.get(i+1).getFilePos() !=0) {
        //System.out.println(chunkList.get(i).toString() + " ---" + count + "--- " + chunkList.get(i+1).toString());
        /*System.out.println(" Min : " + min + " ; Max : " + max + " ; Size : " + size + " ; Chunk count : " + count);
        min = Long.MAX_VALUE;
        max = 0;
        size = 0;*/
        count = 0;
      } else {
        //System.out.println(chunkList.get(i));
      }
    }

    System.out.println(" Min : " + min + " ; Max : " + max + " ; Size : " + size + " ; Gap : " + ((max-min)*1.0/1024/1024));
  }

  public static void main(String[] args) {
    String fPath = "/Users/feihu/Documents/Data/Merra2/MERRA2_100.inst1_2d_int_Nx.19800101.nc4";
    //String fPath = "/Users/feihu/Documents/Data/Merra/MERRA300.prod.simul.tavgM_2d_mld_Nx.201306.hdf";
    try {
      NetcdfFile ncfile = NetcdfFile.open(fPath);
      for (int i=3; i< 11; i++) {
        Variable variable = ncfile.getVariables().get(i);
        H5Chunk.checkChunk(variable, ncfile);
        //System.out.println("******" + variable.toString());
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InvalidRangeException e) {
      e.printStackTrace();
    }
  }
}
