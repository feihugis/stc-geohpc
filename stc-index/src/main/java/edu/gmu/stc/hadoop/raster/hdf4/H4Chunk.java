package edu.gmu.stc.hadoop.raster.hdf4;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

import edu.gmu.stc.hadoop.raster.ChunkUtils;
import edu.gmu.stc.hadoop.raster.DataChunk;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

/**
 * Created by Fei Hu on 8/24/16.
 */
public class H4Chunk extends DataChunk {
  public H4Chunk() {
  }

  public H4Chunk(int[] corner, int[] shape, String[] dimensions, long filePos, long byteSize,
                 int filterMask, String[] hosts, String dataType, String shortName, String filePath, int time) {
    super(corner, shape, dimensions, filePos, byteSize, filterMask, hosts, dataType, shortName, filePath, time);
  }

  /*
  byte[] buffer = new byte[vinfo.length];
    raf.seek(vinfo.start);
    raf.readFully(buffer);
    ByteArrayInputStream in = new ByteArrayInputStream(buffer);
    return new java.util.zip.InflaterInputStream(in);
  * */

  public static void main(String[] args) {
    String fPath = "/Users/feihu/Documents/Data/modis_hdf/MOD08_D3/MOD08_D3.A2016001.006.2016008061022.hdf";
    //String fPath = "/Users/feihu/Documents/Data/Merra/MERRA300.prod.simul.tavgM_2d_mld_Nx.201306.hdf";
    try {
      NetcdfFile ncfile = NetcdfFile.open(fPath);
      for (int i=30; i< 36; i++) {
        Variable variable = ncfile.getVariables().get(i);
        ChunkUtils.geneDataChunks(variable, "hdf4", fPath);
        System.out.println(variable.getShortName());
        System.out.println("******" + variable.getVarLocationInformation());
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InvalidRangeException e) {
      e.printStackTrace();
    }
  }
}
