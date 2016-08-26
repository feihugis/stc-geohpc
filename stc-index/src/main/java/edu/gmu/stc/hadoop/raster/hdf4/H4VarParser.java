package edu.gmu.stc.hadoop.raster.hdf4;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.gmu.stc.hadoop.raster.DataChunk;
import edu.gmu.stc.hadoop.raster.VarLayoutParser;
import edu.gmu.stc.hadoop.raster.hdf5.H5Chunk;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Variable;

/**
 * Created by Fei Hu on 8/24/16.
 */
public class H4VarParser extends VarLayoutParser {
  private static final Log LOG = LogFactory.getLog(H4VarParser.class);

  @Override
  public List<DataChunk> layoutParse(Variable var, String filePath, FileSystem fs) {
    List<DataChunk> chunkList = new ArrayList<DataChunk>();
    try {
      String[] chunks = var.getVarLocationInformation().split(";");
      for (String chunk : chunks) {
        H4Chunk h4Chunk = chunkInfoParser(var.getShape(), chunk, var.getDimensionsString(), var.getShortName(), var.getDataType().toString(), filePath, fs);
        if (h4Chunk != null) {
          chunkList.add(h4Chunk);
        }
      }
      return chunkList;
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InvalidRangeException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * TODO: for the compression java.util.zip.InflaterInputStream, we assume it as 999
   * @param metaInfo
   * @param dimensions
   * @param varShortName
   * @param filePath
   * @param dataType
   * @return
   *
   * example: 0:179,0:359 start at 1546442, length is 92089, compress code is java.util.zip.InflaterInputStream;
   * @throws IOException
   */

  private H4Chunk chunkInfoParser(int[] varShape, String metaInfo, String dimensions, String varShortName, String dataType,
                                    String filePath, FileSystem fs) throws IOException {
    //TODO: for some special variable, the extended netcdf library does not support them, and the library need be improved.
    if (metaInfo.contains("Do not support this format of data")) {
      LOG.error(String.format("The extended NetCDF library does not support the variable %1$s in %2$s", varShortName, filePath));
      return null;
    }
    String[] parcels =  metaInfo.split(" ");
    String cnr = parcels[0];
    String[] cnrs = cnr.split(",");
    int corners[] = new int[cnrs.length];
    int shape[] = new int[cnrs.length];
    for (int i=0; i<cnrs.length; i++) {
      String[] r = cnrs[i].split(":");
      corners[i] = Integer.parseInt(r[0]);
      shape[i] = Integer.parseInt(r[1]) - Integer.parseInt(r[0]) + 1;
    }

    String filePosS = subString(metaInfo, "start at ", ", length");
    long filePos = Long.parseLong(filePosS);

    String byteSizeS = subString(metaInfo, "length is ", ", compress code");
    long byteSize = Long.parseLong(byteSizeS);

    String filterMaskS = parcels[parcels.length-1];
    int filterMask;

    if (filterMaskS.equals("java.util.zip.InflaterInputStream")) {
      filterMask = 999;
    } else {
      filterMask = Integer.parseInt(filterMaskS);
    }

    String[] dims = dimensions.split(" ");

    //For MOD08_D3
    //TODO: need support more datasets
    String timeS = "";
    if (filePath.contains("MOD08_D3")) {
      timeS = filePath.split("\\.A")[1].substring(0,7);
    }

    int time = Integer.parseInt(timeS);

    BlockLocation[] blockLocations = fs.getFileBlockLocations(fs.getFileLinkStatus(new Path(filePath)), filePos, byteSize);
    List<String> hosts = new ArrayList<String>();
    for (BlockLocation blck: blockLocations) {
      hosts.addAll(Arrays.asList(blck.getHosts()));
    }
    String[] hsts = new String[hosts.size()];
    hosts.toArray(hsts);

    String geometryInfo = getGeometryInfo(varShape, corners, shape);

    H4Chunk h4Chunk = new H4Chunk(corners, shape, dims, filePos, byteSize, filterMask,
                              hsts, dataType, varShortName, filePath, time, geometryInfo);
    return h4Chunk;
  }
}
