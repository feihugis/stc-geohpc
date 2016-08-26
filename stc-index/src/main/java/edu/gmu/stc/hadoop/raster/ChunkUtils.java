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

  public static List<String> getAllVarShortNames(String fileInput, String groupName) {
    Path path = new Path(fileInput);
    List<String> varsList = new ArrayList<String>();
    try {
      NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(path), fs.getConf());
      NetcdfFile ncfile = NetcdfFile.open(raf, path.toString());

      // null means no group in this datasat
      if (groupName == null) {
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
