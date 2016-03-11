package edu.gmu.stc.hadoop.raster.hdf5;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import edu.gmu.stc.database.IndexOperator;
import edu.gmu.stc.hadoop.index.tools.Utils;
import edu.gmu.stc.hadoop.raster.index.merra2.Merr2IndexBuilder;
import edu.gmu.stc.hadoop.vector.Polygon;

/**
 * Created by Fei Hu on 3/9/16.
 */
public class H5FileInputFormat extends FileInputFormat {
  private static final Log LOG = LogFactory.getLog(H5FileInputFormat.class);

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    LOG.info("**************************  Start to getSplits");
    long startTime = System.currentTimeMillis();
    int numDims = 0;

    List<InputSplit> inputSplits = new ArrayList<InputSplit>();
    IndexOperator indexOptr = new IndexOperator();

    List<String> varNamesList = Arrays.asList(job.getConfiguration().getStrings("variables"));
    int startDate = Integer.parseInt(job.getConfiguration().get("startTime")); //For daily data, the format should be 20141001 (yyyymmdd)
    int endDate = Integer.parseInt(job.getConfiguration().get("endTime"));

    //String inputBbox = job.getConfiguration().get("bbox");
    //HashMap<String, List<Integer[]>> bboxMap = Utils.parseBbox(inputBbox);
    //List<Integer[]> startList = new ArrayList<Integer[]>(bboxMap.get("start"));
    //List<Integer[]> endList = new ArrayList<Integer[]>(bboxMap.get("end"));

    List<FileStatus> fileStatusList = listStatus(job);

    Merr2IndexBuilder merra2IndexBuilder = new Merr2IndexBuilder();
    Polygon plgn = new Polygon(new double[]{0.0, 1.0, 1.0, 0.0}, new double[]{0.0, 0.0, 1.0, 1.0}, 4);
    inputSplits.addAll(merra2IndexBuilder.queryDataChunksByinputFileStatus(fileStatusList, varNamesList, plgn));
    return inputSplits;
  }

  @Override
  protected List<FileStatus> listStatus(JobContext jobC) throws IOException {
    int startTime = Integer.parseInt(jobC.getConfiguration().get("startTime"));
    int endTime = Integer.parseInt(jobC.getConfiguration().get("endTime"));

    List<FileStatus> files = super.listStatus(jobC);

    //Filter the files that is not netcdf files (netcdf or hdf)
    ArrayList<FileStatus> unNetCDFs = new ArrayList<FileStatus>();
    for(FileStatus file: files) {
      String path = file.getPath().toString();
      String[] paths = path.split("\\.");
      String format = paths[paths.length-1];
      if(!(format.equalsIgnoreCase("hdf") || format.equalsIgnoreCase("nc4"))) {
        unNetCDFs.add(file);
      }
    }

    files.removeAll(unNetCDFs);

    ArrayList<FileStatus> nfiles = new ArrayList<FileStatus>();
    for (FileStatus file: files) {
      String path = file.getPath().toString();
      String[] paths = path.split("\\.");
      String time = paths[paths.length - 2];
      int timeValue = Integer.parseInt(time);
      if(timeValue<startTime || timeValue>endTime) {
        System.out.println(file.getPath().toString());
        nfiles.add(file);
      }
    }
    files.removeAll(nfiles);
    return files;
  }

  @Override
  public org.apache.hadoop.mapreduce.RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    H5ChunkReader recordReader = new H5ChunkReader();
    return recordReader;
  }
}
