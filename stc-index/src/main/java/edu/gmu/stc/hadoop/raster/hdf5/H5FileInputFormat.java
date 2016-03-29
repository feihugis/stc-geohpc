package edu.gmu.stc.hadoop.raster.hdf5;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.postgis.PGgeometry;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import edu.gmu.stc.database.IndexOperator;
import edu.gmu.stc.hadoop.index.tools.Utils;
import edu.gmu.stc.hadoop.raster.RasterUtils;
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

    List<String> varNamesList = Arrays.asList(job.getConfiguration().getStrings("variables"));
    int startDate = Integer.parseInt(job.getConfiguration().get("startTime")); //For daily data, the format should be 20141001 (yyyymmdd)
    int endDate = Integer.parseInt(job.getConfiguration().get("endTime"));
    List<FileStatus> fileStatusList = listStatus(job);
    fileStatusList = RasterUtils.filterMerraInputFilesByTime(fileStatusList, startDate, endDate);

    String inputBbox = job.getConfiguration().get("bbox");
    HashMap<String, List<Integer[]>> bboxMap = Utils.parseBbox(inputBbox);
    List<Integer[]> startCorners = new ArrayList<Integer[]>(bboxMap.get("start"));
    List<Integer[]> endCorners = new ArrayList<Integer[]>(bboxMap.get("end"));

    /*List<Integer[]> subStartCorners = new ArrayList<Integer[]>();
    List<Integer[]> subEndCorners = new ArrayList<Integer[]>();

    //subset the dims except the lat and lon dimension. Here assume the lat and lon dims are the last two dimensions.
    for (int i=0; i<startCorners.size(); i++) {
      Integer[] start = startCorners.get(i);
      Integer[] end = endCorners.get(i);
      Integer[] subStart = new Integer[start.length-2];
      Integer[] subEnd = new Integer[end.length-2];
      for (int j=0; j<subStart.length; j++) {
        subStart[j] = start[j];
        subEnd[j] = end[j];
      }
      subStartCorners.add(subStart);
      subEndCorners.add(subEnd);
    }*/

    String geoBBox = job.getConfiguration().get("geoBBox");
    Polygon geoPolygon = null;
    try {
      geoPolygon = Polygon.generatePolygonFromPGgeometry(geoBBox);
    } catch (SQLException e) {
      e.printStackTrace();
    }

    Merr2IndexBuilder merra2IndexBuilder = new Merr2IndexBuilder();
    //Polygon plgn = new Polygon(new double[]{-180.0, -180.0, 180.0, 180.0}, new double[]{-90.0, 90.0, 90.0, -90.0}, 4);
    //inputSplits.addAll(merra2IndexBuilder.queryDataChunksByinputFileStatus(fileStatusList, varNamesList, geoPolygon));
    inputSplits.addAll(merra2IndexBuilder.queryDataChunksByinputFileStatus(fileStatusList, varNamesList, geoPolygon, startCorners, endCorners));
    return inputSplits;
  }

  @Override
  protected List<FileStatus> listStatus(JobContext jobC) throws IOException {

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

    return files;
  }

  @Override
  public org.apache.hadoop.mapreduce.RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    H5ChunkReader recordReader = new H5ChunkReader();
    return recordReader;
  }
}
