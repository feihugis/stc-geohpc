package edu.gmu.stc.hadoop.raster.hdf5;

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
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Variable;

/**
 * Created by Fei Hu on 8/24/16.
 */
public class H5VarParser extends VarLayoutParser {
  private static final Log LOG = LogFactory.getLog(H5VarParser.class);

  @Override
  public List<DataChunk> layoutParse(Variable var, String filePath, FileSystem fs) {
    List<DataChunk> chunkList = new ArrayList<DataChunk>();
    try {
      String[] chunks = var.getVarLocationInformation().split(";");
      for (String chunk : chunks) {
        chunkList.add(h5ChunkInfoParser(var.getShape(), chunk, var.getDimensionsString(), var.getShortName(), var.getDataType().toString(), filePath, fs));
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
   * TODO: fix the time extraction, here it treat the last second one as the time info
   * @param metaInfo
   * @param dimensions
   * @param varShortName
   * @param filePath
   * @param dataType
   * @return
   *
   * example: 0:0,0:90,288:431  ChunkedDataNode size=37348 filterMask=0 filePos=72345715 offsets= 0 0 288 0
   * @throws IOException
   */
  private H5Chunk h5ChunkInfoParser(int[] varShape, String metaInfo, String dimensions, String varShortName, String dataType,
                                    String filePath, FileSystem fs) throws IOException {
    String cnr = metaInfo.substring(0, metaInfo.indexOf("  ChunkedDataNode"));
    String[] cnrs = cnr.split(",");
    int corners[] = new int[cnrs.length];
    int shape[] = new int[cnrs.length];
    for (int i=0; i<cnrs.length; i++) {
      String[] r = cnrs[i].split(":");
      corners[i] = Integer.parseInt(r[0]);
      shape[i] = Integer.parseInt(r[1]) - Integer.parseInt(r[0]) + 1;
    }
    String[] dims = dimensions.split(" ");

    int time = 0;
    //TODO: need support more data set
    if (filePath.contains("MERRA")) {
      String[] tmps = filePath.split("\\.");
      time = Integer.parseInt(tmps[tmps.length-2]);
    } else {
      LOG.error("Do not support this time interpretation in " + filePath);
    }

    String byteSizeIn = subString(metaInfo, "size=", " filterMask=");
    String filterMaskIn = subString(metaInfo, "filterMask=", " filePos=");
    String filePosIn = subString(metaInfo, "filePos=", " offsets");

    long byteSize = Long.parseLong(byteSizeIn);
    long filePos = Long.parseLong(filePosIn);
    int filterMask = Integer.parseInt(filterMaskIn);

    BlockLocation[] blockLocations = fs.getFileBlockLocations(fs.getFileLinkStatus(new Path(filePath)), filePos, byteSize);
    List<String> hosts = new ArrayList<String>();
    for (BlockLocation blck: blockLocations) {
      hosts.addAll(Arrays.asList(blck.getHosts()));
    }
    String[] hsts = new String[hosts.size()];
    hosts.toArray(hsts);

    String geometryInfo = getGeometryInfo(varShape, corners, shape);

    H5Chunk h5Chunk = new H5Chunk(varShortName, filePath, corners, shape, dims, filePos, byteSize, filterMask,
                                  hsts, dataType, time, geometryInfo);
    return h5Chunk;
  }
}