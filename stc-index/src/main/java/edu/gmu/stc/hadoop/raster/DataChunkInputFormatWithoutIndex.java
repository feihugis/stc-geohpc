package edu.gmu.stc.hadoop.raster;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Fei Hu on 4/19/16.
 *
 * For the job configuration, it need two variables: fileFormats (String[]), dataType(String) as inputs.
 */
public class DataChunkInputFormatWithoutIndex extends FileInputFormat {
  String[] fileFormats;
  int startDate;
  int endDate;

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
     List<InputSplit> inputSplits = new ArrayList<InputSplit>();

     fileFormats = job.getConfiguration().getStrings("fileFormats");
     String dataType = job.getConfiguration().get("dataType");
     String[] vars = job.getConfiguration().getStrings("variables");
     String[] cornerString = job.getConfiguration().getStrings("corner");
     String[] shapeString = job.getConfiguration().getStrings("shape");
     String[] dimensions = job.getConfiguration().getStrings("dimension");
     int[] corner = stringArrayToIntArray(cornerString);
     int[] shape = stringArrayToIntArray(shapeString);
     startDate = job.getConfiguration().getInt("startDate", 0);
     endDate = job.getConfiguration().getInt("endDate", 0);


     List<LocatedFileStatus> fileStatusList = listStatus(job);
     for (LocatedFileStatus file : fileStatusList) {
       String path = file.getPath().toString();
       //String shortName = "Total_precipitation_surface_12_Hour_120";
       String filePath = path;
       long filePos = -1;
       long byteSize = -1;
       int filterMask = -1;
       List<String> hostList = new ArrayList<String>();
       BlockLocation[] blockLocations = file.getBlockLocations();
       for (int i=0; i<blockLocations.length; i++) {
         String[] hs = blockLocations[i].getHosts();
         hostList.addAll(Arrays.asList(hs));
       }
       String[] hosts = new String[hostList.size()];
       hosts = hostList.toArray(hosts);
       List<DataChunk> dataChunkList = new ArrayList<DataChunk>();
       for (String varShortName : vars) {
         DataChunk dataChunk = new DataChunk( corner, shape, dimensions, filePos, byteSize, filterMask, hosts, dataType, varShortName, filePath);
         dataChunkList.add(dataChunk);
       }

       DataChunkInputSplit dataChunkInputSplit = new DataChunkInputSplit(dataChunkList);
       inputSplits.add(dataChunkInputSplit);
     }

    System.out.println("+++++++++++++++++++++++ inputsplits " + inputSplits.size());
    return inputSplits;
  }

  @Override
  protected List<LocatedFileStatus> listStatus(JobContext jobC) throws IOException {

    List<LocatedFileStatus> files = super.listStatus(jobC);

    //Filter the files that does not match the fileformat
    ArrayList<LocatedFileStatus> unreferredList = new ArrayList<LocatedFileStatus>();
    for(LocatedFileStatus file: files) {
      String path = file.getPath().toString();
      boolean isQueried = false;
      for (String format: this.fileFormats) {
        if(path.endsWith(format)) {
          isQueried = true;
          break;
        }
      }
      if (!isQueried) {
        unreferredList.add((LocatedFileStatus) file);
      }
    }
    files.removeAll(unreferredList);

    /*filter out the files that are out of dates
    TODO: fix the time filter pattern
    Here assume that the date information is in the last or second last part of the string array splitted by "\\."*/
    List<LocatedFileStatus> outOfdate = new ArrayList<LocatedFileStatus>();
    if (startDate!=0 && endDate!=0) {
      for (LocatedFileStatus f : files) {
        String in = f.getPath().toString();
        String[] tmps = in.split("\\.");

        if (tmps[tmps.length-1].length() == (startDate+"").length()) {
          int time = Integer.parseInt(tmps[tmps.length-1]);
          if (time>=startDate && time<=endDate) {
            continue;
          } else {
            outOfdate.add(f);
          }
        } else {
          int time = Integer.parseInt(tmps[tmps.length-2]);
          if (time>=startDate && time<=endDate) {
            continue;
          } else {
            outOfdate.add(f);
          }
        }
      }
    }

    files.removeAll(outOfdate);
    return files;
  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new DataChunkReaderWithoutIndex();
  }

  public int[] stringArrayToIntArray(String[] input) {
    int[] result = new int[input.length];
    for (int i=0; i<input.length; i++) {
      result[i] = Integer.parseInt(input[i]);
    }
    return result;
  }
}
