package edu.gmu.stc.hadoop.raster.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.qjournal.client.IPCLoggerChannel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;

import edu.gmu.stc.hadoop.raster.DataChunk;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.util.IO;

/**
 * Created by Fei Hu on 8/26/16.
 */
public class DataChunkIOProvider {
  private static final Log LOG = LogFactory.getLog(DataChunkIOProvider.class);

  public static Array read(DataChunk dataChunk, FSDataInputStream inputStream) throws IOException {
    byte[] compressedByteArray = new byte[(int) dataChunk.getByteSize()];
    long filePos = dataChunk.getFilePos();
    int n = inputStream.read(filePos, compressedByteArray, 0, compressedByteArray.length);

    return read(dataChunk, compressedByteArray);
  }

  public static Array read(DataChunk dataChunk, byte[] compressedByteArray) throws IOException {
        //0: Merra2; 4: Merra-1; 999: Modis08_3D
    switch (dataChunk.getFilterMask()) {
      case 999:
        return read_999(dataChunk, compressedByteArray);

      case 0:
        return read_0(dataChunk, compressedByteArray);

      case 4:
        return read_4(dataChunk, compressedByteArray);

    }

    return null;
  }

  private static Array read_999(DataChunk dataChunk, byte[] compressedByteArray) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(compressedByteArray);
    InputStream zin = new java.util.zip.InflaterInputStream(in);
    DataInputStream delegate;
    if (zin instanceof DataInputStream)
      delegate = (DataInputStream) zin;
    else
      delegate = new DataInputStream(zin);

    if (dataChunk.getDataType().equals("short")) {
      short[] pa = new short[dataChunk.getShapeSize()];
      for (int i=0; i<pa.length; i++) {
        pa[i] = delegate.readShort();
      }
      return Array.factory(DataType.getType(dataChunk.getDataType()), dataChunk.getShape(), pa);
    }

    if (dataChunk.getDataType().equals("int")) {
      int[] pa = new int[dataChunk.getShapeSize()];
      for (int i=0; i<pa.length; i++) {
        pa[i] = delegate.readInt();
      }
      return Array.factory(DataType.getType(dataChunk.getDataType()), dataChunk.getShape(), pa);
    }

    if (dataChunk.getDataType().equals("float")) {
      float[] pa = new float[dataChunk.getShapeSize()];
      for (int i=0; i<pa.length; i++) {
        pa[i] = delegate.readFloat();
      }
      return Array.factory(DataType.getType(dataChunk.getDataType()), dataChunk.getShape(), pa);
    }

    if (dataChunk.getDataType().equals("double")) {
      double[] pa = new double[dataChunk.getShapeSize()];
      for (int i=0; i<pa.length; i++) {
        pa[i] = delegate.readDouble();
      }
      return Array.factory(DataType.getType(dataChunk.getDataType()), dataChunk.getShape(), pa);
    }

    return null;
  }

  public static Array read_0(DataChunk dataChunk, byte[] compressedByteArray) throws IOException {
    compressedByteArray = inflate(compressedByteArray);
    int typeLength = sizeof(DataType.getType(dataChunk.getDataType()).getClassType());
    compressedByteArray = shuffle(compressedByteArray, typeLength);
    ByteBuffer bb = ByteBuffer.wrap(compressedByteArray);
    ByteOrder byteOrder = ByteOrder.LITTLE_ENDIAN;
    bb.order(byteOrder);

    return convertByteBuffer2Array(bb, dataChunk.getDataType(), dataChunk.getShape());

  }

  public static Array read_4(DataChunk dataChunk, byte[] compressedByteArray) throws IOException {
    InputStream in = new ByteArrayInputStream(compressedByteArray);
    InputStream zin = new java.util.zip.InflaterInputStream(in);
    int typeLength = sizeof(DataType.getType(dataChunk.getDataType()).getClassType());
    ByteArrayOutputStream out = new ByteArrayOutputStream(dataChunk.getShapeSize()*typeLength);
    IO.copy(zin, out);
    byte[] buffer = out.toByteArray();
    ByteBuffer bb = ByteBuffer.wrap(buffer);

    return convertByteBuffer2Array(bb, dataChunk.getDataType(), dataChunk.getShape());
  }

  private static Array convertByteBuffer2Array(ByteBuffer bb, String dataType, int[] shape) {
    if (dataType.equals("short")) {
      ShortBuffer valBuffer = bb.asShortBuffer();
      short[] pa = new short[valBuffer.capacity()];
      for (int i = 0; i < pa.length; i++) {
        pa[i] = valBuffer.get(i);
      }
      return Array.factory(DataType.getType(dataType), shape, pa);
    }

    if (dataType.equals("int")) {
      IntBuffer valBuffer = bb.asIntBuffer();
      int[] pa = new int[valBuffer.capacity()];
      for (int i = 0; i < pa.length; i++) {
        pa[i] = valBuffer.get(i);
      }
      return Array.factory(DataType.getType(dataType), shape, pa);
    }

    if (dataType.equals("float")) {
      FloatBuffer valBuffer = bb.asFloatBuffer();
      float[] pa = new float[valBuffer.capacity()];
      for (int i = 0; i < pa.length; i++) {
        pa[i] = valBuffer.get(i);
      }
      return Array.factory(DataType.getType(dataType), shape, pa);
    }

    if (dataType.equals("double")) {
      DoubleBuffer valBuffer = bb.asDoubleBuffer();
      double[] pa = new double[valBuffer.capacity()];
      for (int i = 0; i < pa.length; i++) {
        pa[i] = valBuffer.get(i);
      }
      return Array.factory(DataType.getType(dataType), shape, pa);
    }

    LOG.error("Does not support this data type : " + dataType + "  in class: " + DataChunkIOProvider.class.getName());

    return null;
  }

  /**
   * inflate data
   *
   * @param compressed compressed data
   * @return uncompressed data
   * @throws IOException on I/O error
   */
  private static byte[] inflate(byte[] compressed) throws IOException {
    // run it through the Inflator
    ByteArrayInputStream in = new ByteArrayInputStream(compressed);
    java.util.zip.InflaterInputStream inflater = new java.util.zip.InflaterInputStream(in);
    ByteArrayOutputStream out = new ByteArrayOutputStream(8 * compressed.length);
    IO.copy(inflater, out);

    byte[] uncomp = out.toByteArray();
    return uncomp;
  }

  private static byte[] shuffle(byte[] data, int n) throws IOException {
    assert data.length % n == 0;
    if (n <= 1) return data;

    int m = data.length / n;
    int[] count = new int[n];
    for (int k = 0; k < n; k++) count[k] = k * m;

    byte[] result = new byte[data.length];

    for (int i = 0; i < m; i++) {
      for (int j = 0; j < n; j++) {
        result[i*n+j] = data[i + count[j]];
      }
    }

    return result;
  }

  public static int sizeof(Class dataType) {
    if (dataType == null) throw new NullPointerException();

    if (dataType == int.class    || dataType == Integer.class)   return 4;
    if (dataType == short.class  || dataType == Short.class)     return 2;
    if (dataType == byte.class   || dataType == Byte.class)      return 1;
    if (dataType == char.class   || dataType == Character.class) return 2;
    if (dataType == long.class   || dataType == Long.class)      return 8;
    if (dataType == float.class  || dataType == Float.class)     return 4;
    if (dataType == double.class || dataType == Double.class)    return 8;

    return 4;

  }

}
