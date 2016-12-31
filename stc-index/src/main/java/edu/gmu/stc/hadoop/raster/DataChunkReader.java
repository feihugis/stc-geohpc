package edu.gmu.stc.hadoop.raster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.hadoop.commons.ClimateHadoopConfigParameter;
import edu.gmu.stc.hadoop.raster.io.DataChunkIOProvider;
import edu.gmu.stc.hadoop.raster.io.datastructure.ArraySerializer;
import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;

/**
 * Created by Fei Hu on 8/26/16.
 */
public class DataChunkReader extends RecordReader<DataChunk, ArraySerializer> {
  private static final Log LOG = LogFactory.getLog(DataChunkReader.class);

  private List<DataChunk> dataChunkList = new ArrayList<DataChunk>();
  private int currentKeyMark = -1;
  private int keySize = 0;
  private boolean debug= false;
  private FSDataInputStream inputStream;
  private Configuration conf;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws
                                                                                       IOException, InterruptedException {
    DataChunkInputSplit dataChunkInputSplit = (DataChunkInputSplit) inputSplit;
    dataChunkList = dataChunkInputSplit.getChunkList();
    keySize = dataChunkList.size();
    conf = taskAttemptContext.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    DataChunk dataChunk = this.dataChunkList.get(0);
    Path path = new Path(dataChunk.getFilePath());
    inputStream = fs.open(path);
  }

  public void initialize(InputSplit inputSplit, Configuration conf) throws
                                                                    IOException, InterruptedException {
    DataChunkInputSplit dataChunkInputSplit = (DataChunkInputSplit) inputSplit;
    dataChunkList = dataChunkInputSplit.getChunkList();
    keySize = dataChunkList.size();
    FileSystem fs = FileSystem.get(conf);
    DataChunk dataChunk = this.dataChunkList.get(0);
    Path path = new Path(dataChunk.getFilePath());
    inputStream = fs.open(path);
  }



  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    currentKeyMark++;
    if (currentKeyMark < keySize) {
      return true;
    } else {
      return false;
    }
  }

  //TODO: need improve the efficiency
  @Override
  public DataChunk getCurrentKey() throws IOException, InterruptedException {
    DataChunk dataChunk = this.dataChunkList.get(currentKeyMark);
    DataChunk key = new DataChunk();

    //TODO: here just set a few attributes for the key datachunk, if need more attributes, can add more here
    //here we do not directly return the original datachunk, because it need be updated the corner and shape info,
    // which will affect the getValue() funtion
    key.setVarShortName(dataChunk.getVarShortName());
    key.setCorner(dataChunk.getCorner());
    key.setShape(dataChunk.getShape());
    key.setDataType(dataChunk.getDataType());
    key.setHosts(dataChunk.getHosts());
    key.setTime(dataChunk.getTime());

    int[] queryCorner = conf.getInts(ClimateHadoopConfigParameter.QUERY_SPACE_CORNER);
    int[] queryShape = conf.getInts(ClimateHadoopConfigParameter.QUERY_SPACE_SHAPE);

    int[] targetCorner = new int[queryCorner.length], targetShape = new int[queryCorner.length];
    if (ChunkUtils.getIntersection(dataChunk.getCorner(), dataChunk.getShape(), queryCorner, queryShape, targetCorner, targetShape)) {
      key.setCorner(targetCorner);
      key.setShape(targetShape);
      return key;
    } else {
      return null;
    }
  }

  //TODO: need improve the efficiency
  @Override
  public ArraySerializer getCurrentValue() throws IOException, InterruptedException {
    DataChunk dataChunk = this.dataChunkList.get(currentKeyMark);
    ArraySerializer value = null;

    int[] queryCorner = conf.getInts(ClimateHadoopConfigParameter.QUERY_SPACE_CORNER);
    int[] queryShape = conf.getInts(ClimateHadoopConfigParameter.QUERY_SPACE_SHAPE);

    int[] targetCorner = new int[queryCorner.length], targetShape = new int[queryCorner.length];

    if (queryCorner != null && queryShape != null) {
      // the query bounding box is same with datachunk
      if (ChunkUtils.isSameBoundingBox(dataChunk.getCorner(), dataChunk.getShape(), queryCorner, queryShape, targetCorner, targetShape)) {
        Array array = DataChunkIOProvider.read(dataChunk, inputStream);
        value = ArraySerializer.factory(array);
        return value;
      }

      // the query bounding box is different from datachunk but intersected
      if (ChunkUtils.getIntersection(dataChunk.getCorner(), dataChunk.getShape(), queryCorner, queryShape, targetCorner, targetShape)) {
        Array array = DataChunkIOProvider.read(dataChunk, inputStream);
        int[] relativeCorner = new int[targetCorner.length];
        for (int i = 0; i < relativeCorner.length; i++) {
          relativeCorner[i] = targetCorner[i] - dataChunk.getCorner()[i];
        }
        try {
          //note that section function only does logical subseting, but refers to  the same array;
          // so we need use copy function to generate a physical subsetting array
          array = array.section(relativeCorner, targetShape).copy();
          if (array.getShape().length != relativeCorner.length) {
            array = array.reshape(targetShape);
          }
        } catch (InvalidRangeException e) {
          LOG.error(e.toString());
          e.printStackTrace();
        }
        value = ArraySerializer.factory(array);
        return value;
      } else {
        return null;
      }
    } else {
      Array array = DataChunkIOProvider.read(dataChunk, inputStream);
      value = ArraySerializer.factory(array);
      return value;
    }
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return currentKeyMark*1.0f/keySize;
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }
}
