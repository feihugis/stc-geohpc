package edu.gmu.stc.hadoop.raster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.gmu.stc.configure.MyProperty;
import edu.gmu.stc.hadoop.index.io.merra.NcHdfsRaf;
import edu.gmu.stc.hadoop.raster.hdf5.H5Chunk;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

/**
 * Created by Fei Hu on 2/17/16.
 */
public class ChunkFactory {
  private static FileSystem fs = null;
  private FSDataInputStream fsInput = null;
  private static Configuration conf = new Configuration();
  static {
    conf.set("fs.defaultFS", MyProperty.nameNode);
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static List<DataChunk> geneDataChunks(Variable var, String dataFormat, String filePath) throws IOException, InvalidRangeException {
    if (dataFormat.equals("netCDF-4") || dataFormat.equals("nc4") || dataFormat.equals("hdf-5") || dataFormat.equals("hdf5")) {
      return  h5VarLocInfoParser(var, filePath);
    }

    return null;
  }

  //0:0,0:90,0:143  ChunkedDataNode size=37709 filterMask=0 filePos=72270231 offsets= 0 0 0 0 ;
  //0:0,0:90,144:287  ChunkedDataNode size=37775 filterMask=0 filePos=72307940 offsets= 0 0 144 0 ;
  //0:0,0:90,288:431  ChunkedDataNode size=37348 filterMask=0 filePos=72345715 offsets= 0 0 288 0 ;
  public static List<DataChunk> h5VarLocInfoParser(Variable var, String filePath) throws IOException, InvalidRangeException {
    List<DataChunk> chunkList = new ArrayList<DataChunk>();
    String[] chunks = var.getVarLocationInformation().split(";");
    for (String chunk : chunks) {
      //System.out.println(chunk);
      chunkList.add(h5ChunkInfoParser(chunk, var.getDimensionsString(), var.getShortName(), filePath, var.getDataType().toString()));
    }
    return chunkList;
  }

  //0:0,0:90,288:431  ChunkedDataNode size=37348 filterMask=0 filePos=72345715 offsets= 0 0 288 0
  public static H5Chunk h5ChunkInfoParser(String metaInfo, String dimensions, String varShortName, String filePath, String dataType)
      throws IOException {
    String cnr = metaInfo.substring(0, metaInfo.indexOf("  ChunkedDataNode"));

    String byteSizeIn = subString(metaInfo, "size=", " filterMask=");
    String filterMaskIn = subString(metaInfo, "filterMask=", " filePos=");
    String filePosIn = subString(metaInfo, "filePos=", " offsets");

    String[] cnrs = cnr.split(",");
    int corners[] = new int[cnrs.length];
    int shape[] = new int[cnrs.length];
    for (int i=0; i<cnrs.length; i++) {
      String[] r = cnrs[i].split(":");
      corners[i] = Integer.parseInt(r[0]);
      shape[i] = Integer.parseInt(r[1]) - Integer.parseInt(r[0]) + 1;
    }

    long byteSize = Long.parseLong(byteSizeIn);
    long filePos = Long.parseLong(filePosIn);
    int filterMask = Integer.parseInt(filterMaskIn);

    String[] dims = dimensions.split(" ");

    BlockLocation[] blockLocations = fs.getFileBlockLocations(fs.getFileLinkStatus(new Path(filePath)), filePos, byteSize);
    List<String> hosts = new ArrayList<String>();
    for (BlockLocation blck: blockLocations) {
      hosts.addAll(Arrays.asList(blck.getHosts()));
    }
    String[] hsts = new String[hosts.size()];
    hosts.toArray(hsts);
    H5Chunk h5Chunk = new H5Chunk(varShortName, filePath, corners, shape, dims, filePos, byteSize, filterMask,
                                  hsts, dataType);
    return h5Chunk;
  }

  public static List<DataChunk> geneDataChunks(Path path, String fileFormat) {
    List<DataChunk> chunkList = new ArrayList<DataChunk>();
    try {
      NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(path), fs.getConf());
      NetcdfFile ncfile = NetcdfFile.open(raf, path.toString());
      List<Variable> variableList = ncfile.getVariables();
      for (Variable var : variableList) {
        chunkList.addAll(geneDataChunks(var, "nc4", path.toString()));
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InvalidRangeException e) {
      e.printStackTrace();
    }
    return chunkList;
  }

  /**
   * get the subString between 'start' and 'end' in the 'input'
   * @param input the string to be subsetted
   * @param start the start chars
   * @param end   the end chars
   * @return
   */
  public static String subString(String input, String start, String end) {
    return input.substring(input.indexOf(start) + start.length(), input.indexOf(end));
  }


}
