package edu.gmu.stc.datavisualization.netcdf;

import com.sun.imageio.plugins.gif.GIFImageWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import javax.imageio.ImageIO;
import javax.imageio.stream.FileImageOutputStream;
import javax.imageio.stream.ImageOutputStream;

import edu.gmu.stc.configure.MyProperty;
import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import ucar.ma2.Index;
import ucar.ma2.Index2D;

/**
 * Created by Fei Hu on 3/13/16.
 */
public class PngFactory {
  static FileSystem fs=null;

  static {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", MyProperty.nameNode);
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }


  private static final Font font = new Font("", Font.BOLD, 10);

  public static void drawPNG(Array value, String outputFile, float minValue, float maxValue, String lable, int scale) {
    try {
      MutableImage mi = new MutableImage((BufferedImage) getImage(value, minValue, maxValue, lable, scale));

      BufferedImage originalImage = mi.getImage();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ImageIO.write(originalImage, "png", baos );
      baos.flush();
      byte[] imageInByte = baos.toByteArray();

      FSDataOutputStream hdfsWriter = fs.create(new Path(outputFile));
      hdfsWriter.write(imageInByte);
      hdfsWriter.flush();
      hdfsWriter.close();

      baos.close();


      //Output to local:
      //PngWriter png = new PngWriter(outputName, mi); // create the png
      //png.createMutableImage();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static Image getImage(Array value, float minValue, float maxValue, String lable, int scale) {
    try {

      PngColor color;
      int[] shape = value.getShape(); //assume it is 2-dim array
      int row = shape[0];
      int col = shape[1];
      Index2D index = new Index2D(shape);

      int[][] rgb = new int[row][col];
      color = new PngColor(minValue, maxValue); // create the png color
      // with the dimensions
      // for the legend
      // D2 idw = interpolateArray(oneMonth, scale, thresholdFactor, mv);
      // //interpolate the data
      // shape2d = idw.getShape();

      float val;
      for (int i = 0; i < row; i++) {   //y
        for (int j = 0; j < col; j++) { //x
          val = value.getFloat(index.set(row - 1 - i, j));
          if (val <minValue) {
            rgb[i][j] = -1;
          } else {
            rgb[i][j] = color.getColorRGB(val);
          }
        }
      }

      MutableImage mi = new MutableImage(rgb, col, row);

      if (scale != 1) {
        mi.setImage(mi.resizeBicubic(mi.getImage(),mi.getWidth()*scale, mi.getHeight()*scale));
      }

      if (lable != null) {
        String[] lables = lable.split("_");
        String time = lables[1]+" " + lables[2]+":"+"30";
        DateFormat format = new SimpleDateFormat("yyyyMMdd HH:mm", Locale.ENGLISH);
        Date date = format.parse(time);

        mi.drawString(date.toString(), 15, 5, font, Color.DARK_GRAY);
        mi.combineImagesVertical(mi.getImage(),color.getLegendImage(col*scale, 40, 22, lables[0], null));
      }

      return mi.getImage();

    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public static void geneGIFBilinear(ArrayList<Image> images, String outputName, int scale, int gifInterval) {
    // grab the output image type from the first image in the sequence
    try {
      BufferedImage firstImage = (BufferedImage) images.get(0);

      // create a new BufferedOutputStream with the last argument
      ImageOutputStream output = new FileImageOutputStream(new File(outputName + ".gif"));


      // create a gif sequence with the type of the first image, 1 second
      // between frames, which loops continuously
      GifSequenceWriter writer = new GifSequenceWriter(output, firstImage.getType(), gifInterval, true);

      // write out the first image to our sequence...
      writer.writeToSequence(firstImage);
      for (int i = 1; i < images.size(); i++) {
        BufferedImage nextImage = (BufferedImage) images.get(i);
        writer.writeToSequence(nextImage);
      }

      /*output.flush();

      GIFImageWriter gifImageWriter = (GIFImageWriter) writer.gifWriter;
      FileImageOutputStream originalImage = (FileImageOutputStream) gifImageWriter.getOutput();


      //originalImage.flush();
      //output.flush();
      int offset = originalImage.getBitOffset();
      byte[] gif = new byte[(int) originalImage.length()-100];
      originalImage.read(gif);

      FSDataOutputStream hdfsWriter = fs.create(new Path("1" + output+".gif"));

      hdfsWriter.write(gif);
      hdfsWriter.flush();
      hdfsWriter.close();*/


      writer.close();
      output.close();

      fs.copyFromLocalFile(true, true, new Path(outputName+".gif"), new Path(outputName+"1.gif"));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
