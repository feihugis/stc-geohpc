package edu.gmu.stc.hadoop.raster.hdf5;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;

import edu.gmu.stc.hadoop.raster.DataChunk;
import ucar.nc2.util.IO;

/**
 * Created by Fei Hu on 3/8/16.
 */
public class H5ChunkReader extends RecordReader<DataChunk, ArrayFloatSerializer> {

  private static final Log LOG = LogFactory.getLog(H5ChunkReader.class);
  private Configuration conf;
  private FSDataInputStream inputStream;
  private FileSystem fs;
  private H5ChunkInputSplit h5ChunkInputSplit;
  private int currentKeyMark = -1;
  private int keySize = 0;
  private boolean debug= false;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    this.h5ChunkInputSplit = (H5ChunkInputSplit) inputSplit;
    this.keySize = h5ChunkInputSplit.getChunkList().size();
    this.conf = taskAttemptContext.getConfiguration();
    fs = FileSystem.get(conf);
    inputStream = fs.open(new Path(this.h5ChunkInputSplit.getChunkList().get(0).getFilePath()));
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

  @Override
  public DataChunk getCurrentKey() throws IOException, InterruptedException {
    return h5ChunkInputSplit.getChunkList().get(currentKeyMark);
  }

  /**
   *
   * @return the value in the array style
   * @throws IOException
   * @throws InterruptedException
   * TODO: Fix the uncompressing part; the reading interface is not general since the H5header.Filter info is not extracted yet.
   */
  @Override
  public ArrayFloatSerializer getCurrentValue() throws IOException, InterruptedException {
    byte[] byteArray = new byte[(int) h5ChunkInputSplit.getChunkList().get(currentKeyMark).getByteSize()];
    long filePos = h5ChunkInputSplit.getChunkList().get(currentKeyMark).getFilePos();
    int n = this.inputStream.read(filePos, byteArray, 0, byteArray.length);
    //int filterMask = h5ChunkInputSplit.getChunkList().get(currentKeyMark).getFilterMask();

    byteArray = inflate(byteArray);
    byteArray = shuffle(byteArray, 4);

    /*if (filterMask == 1) {
      byteArray = inflate(byteArray);
    }  else if (filterMask== 3) {
      byteArray = checkfletcher32(byteArray);
    } else
      throw new RuntimeException("Unknown filter type="+filterMask);*/

    ByteBuffer bb = ByteBuffer.wrap(byteArray);
    ByteOrder byteOrder = ByteOrder.LITTLE_ENDIAN;
    bb.order(byteOrder);
    FloatBuffer fltBuffer = bb.asFloatBuffer();
    float[] data = new float[fltBuffer.capacity()];
    for(int i=0; i<data.length; i++) {
      data[i] = fltBuffer.get(i);
    }

    return new ArrayFloatSerializer(h5ChunkInputSplit.getChunkList().get(currentKeyMark).getShape(), data);
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return currentKeyMark*1.0f/keySize;
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }

  /**
   * inflate data
   *
   * @param compressed compressed data
   * @return uncompressed data
   * @throws IOException on I/O error
   */
  private byte[] inflate(byte[] compressed) throws IOException {
    // run it through the Inflator
    ByteArrayInputStream in = new ByteArrayInputStream(compressed);
    java.util.zip.InflaterInputStream inflater = new java.util.zip.InflaterInputStream(in);
    ByteArrayOutputStream out = new ByteArrayOutputStream(8 * compressed.length);
    IO.copy(inflater, out);

    byte[] uncomp = out.toByteArray();
    if (debug) System.out.println(" inflate bytes in= " + compressed.length + " bytes out= " + uncomp.length);
    return uncomp;
  }

  private byte[] shuffle(byte[] data, int n) throws IOException {
    if (debug) System.out.println(" shuffle bytes in= " + data.length + " n= " + n);

    assert data.length % n == 0;
    if (n <= 1) return data;

    int m = data.length / n;
    int[] count = new int[n];
    for (int k = 0; k < n; k++) count[k] = k * m;

    byte[] result = new byte[data.length];
      /* for (int i = 0; i < data.length; i += n) {
        for (int k = 0; k < n; k++) {
          result[count[k]++] = data[i + k];
        }
      } */

    for (int i = 0; i < m; i++) {
      for (int j = 0; j < n; j++) {
        result[i*n+j] = data[i + count[j]];
      }
    }

    return result;
  }

  // just strip off the 4-byte fletcher32 checksum at the end
  private byte[] checkfletcher32(byte[] org) throws IOException {
    byte[] result = new byte[org.length-4];
    System.arraycopy(org, 0, result, 0, result.length);
    if (debug) System.out.println(" checkfletcher32 bytes in= " + org.length + " bytes out= " + result.length);
    return result;
  }
}
