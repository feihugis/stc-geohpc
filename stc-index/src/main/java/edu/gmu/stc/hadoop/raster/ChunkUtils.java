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
import edu.gmu.stc.hadoop.raster.hdf4.H4VarParser;
import edu.gmu.stc.hadoop.raster.hdf5.H5Chunk;
import edu.gmu.stc.hadoop.raster.hdf5.H5ChunkInputSplit;
import edu.gmu.stc.hadoop.raster.hdf5.H5VarParser;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Group;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

/**
 * Created by Fei Hu on 2/17/16.
 */
public class ChunkUtils {
  private static FileSystem fs = null;
  private static Configuration conf = new Configuration();

  static {
    //conf.addResource(new Path(MyProperty.ClimateHadoop_Config_FilePath));
    //conf.set("fs.defaultFS", MyProperty.nameNode);
    //conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    //conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    //conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static List<DataChunk> geneDataChunks(Variable var, String dataFormat, String filePath) throws IOException, InvalidRangeException {
    if (dataFormat.equals("netCDF-4") || dataFormat.equals("nc4") || dataFormat.equals("hdf-5") || dataFormat.equals("hdf5")) {
      return new H5VarParser().layoutParse(var, filePath, fs);
    }

    if (dataFormat.equals("hdf4") || dataFormat.equals("HDF4-EOS")) {
      return new H4VarParser().layoutParse(var, filePath, fs);
    }

    return null;
  }

  public static List<DataChunk> geneDataChunks(Path path, String fileFormat) {
    List<DataChunk> chunkList = new ArrayList<DataChunk>();
    try {
      NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(path), fs.getConf());
      NetcdfFile ncfile = NetcdfFile.open(raf, path.toString());
      String dataformat = ncfile.getFileTypeId();
      List<Variable> variableList = ncfile.getVariables();

      for (Variable var : variableList) {
        chunkList.addAll(geneDataChunks(var, dataformat, path.toString()));
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InvalidRangeException e) {
      e.printStackTrace();
    }
    return chunkList;
  }

  /**
   * TODO: need improve the efficiency
   * @param filePath
   * @param varShortName
   * @return
   */
  public static List<DataChunk> geneDataChunksByVar(String filePath, String varShortName) {
    Path path = new Path(filePath);
    List<DataChunk> chunkList = new ArrayList<DataChunk>();
    try {
      NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(path), fs.getConf());
      NetcdfFile ncfile = NetcdfFile.open(raf, path.toString());
      String fileformat = ncfile.getFileTypeId();
      List<Variable> variableList = ncfile.getVariables();
      for (Variable var : variableList) {
        if (var.getShortName().equals(varShortName)) {
          chunkList.addAll(geneDataChunks(var, fileformat, path.toString()));
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InvalidRangeException e) {
      e.printStackTrace();
    }
    return chunkList;
  }

  public static List<DataChunkInputSplit> generateInputSplitByHosts(List<DataChunk> dataChunkList) {
    List<DataChunkInputSplit> inputSplitList = new ArrayList<DataChunkInputSplit>();
    List<DataChunk> chunkList = new ArrayList<DataChunk>();
    dataChunkList.add(null);
    for(int i=0; i<dataChunkList.size()-1; i++) {
      chunkList.add(dataChunkList.get(i));
      if (ChunkUtils.isShareHosts(dataChunkList.get(i), dataChunkList.get(i+1))
          && dataChunkList.get(i).getFilePath().equals(dataChunkList.get(i+1).getFilePath())) {
        continue;
      } else {
        inputSplitList.add(new DataChunkInputSplit(chunkList));
        chunkList = new ArrayList<DataChunk>();
      }
    }
    return inputSplitList;
  }

  public static List<String> getAllVarShortNames(String fileInput, String groupName) {
    Path path = new Path(fileInput);
    List<String> varsList = new ArrayList<String>();
    try {
      NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(path), fs.getConf());
      NetcdfFile ncfile = NetcdfFile.open(raf, path.toString());

      // null means no group in this datasat
      if (groupName == null || groupName.equals("null")) {
        List<Variable> variableList = ncfile.getVariables();
        for (Variable var : variableList) {
          varsList.add(var.getShortName());
        }
        return varsList;
      }

      List<Group> groupList = ncfile.getRootGroup().getGroups();
      Group group = findGroup(ncfile.getRootGroup(), groupName);
      List<Variable> variableList = group.getVariables();
      for (Variable var : variableList) {
        varsList.add(var.getShortName());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return varsList;
  }

  public static List<String> getSpecifiedVariables(String fileInput, String groupName, String variables) {

    if (variables != null) {
      String[] vars = variables.split(",");
      return Arrays.asList(vars);
    } else {
      return getAllVarShortNames(fileInput, groupName);
    }
  }

  public static Group findGroup(Group rootGroup, String groupName) {
    List<Group> groupList = rootGroup.getGroups();
    Group targetGroup = null;

    for (Group group : groupList) {
      if (group.getShortName().equals(groupName)) {
        targetGroup = group;
        return targetGroup;
      } else {
        targetGroup = findGroup(group, groupName);
        if (targetGroup != null) {
          return targetGroup;
        }
      }
    }

    return targetGroup;
  }

  public static boolean isShareHosts(DataChunk chunk1, DataChunk chunk2) {
    if (chunk2 == null) {
      return false;
    }
    String[] hosts1 = chunk1.getHosts();
    String[] hosts2 = chunk2.getHosts();

    for (int i=0; i<hosts1.length; i++) {
      for (int j = 0; j < hosts2.length; j++) {
        if (hosts1[i].equals(hosts2[j])) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Get the intersection between two input array boundary
   * @param orgCorner
   * @param orgShape
   * @param queryCorner
   * @param queryShape
   * @param targetCorner
   * @param targetShape
   * @return
   */
  public static boolean getIntersection(int[] orgCorner, int[] orgShape, int[] queryCorner, int[] queryShape, int[] targetCorner, int[] targetShape) {
    if (orgCorner.length != queryCorner.length) {
      return  false;
    }

    int[] orgEnd = new int[orgCorner.length], queryEnd = new int[queryCorner.length];

    for (int i=0; i<orgCorner.length; i++) {
      orgEnd[i] = orgCorner[i] + orgShape[i] - 1;
      queryEnd[i] = queryCorner[i] + queryShape[i] - 1;
      if ((orgCorner[i]<queryCorner[i]&&orgEnd[i]<queryCorner[i]) || (orgCorner[i]>queryEnd[i]&&orgEnd[i]>queryEnd[i])) {
        return false;
      }
    }

    int[] targetEnd = new int[orgCorner.length];
    for (int i=0; i<orgCorner.length; i++) {
      targetCorner[i] = Math.max(orgCorner[i], queryCorner[i]);
      targetEnd[i] = Math.min(orgEnd[i], queryEnd[i]);
      targetShape[i] = targetEnd[i] - targetCorner[i] + 1;
    }

    return true;
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
