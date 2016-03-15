package edu.gmu.stc.datavisualization.netcdf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import edu.gmu.stc.configure.MyProperty;
import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import ucar.ma2.Index;
import ucar.ma2.Index2D;

/**
 * Created by Fei Hu on 3/13/16.
 */
public class PngFactory {

  private static final Font font = new Font("", Font.BOLD, 16);

  public static void drawPNG(Array value, String outputFile, float minValue, float maxValue) {
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
          val = value.getFloat(index.set(i,j));
          rgb[i][j] = color.getColorRGB(val);
        }
      }

      MutableImage mi = new MutableImage(rgb, col, row);

      if (false) { // add the date display
        //Calendar c = Calendar.getInstance();
        //c.setTime(date);
        mi.drawString("lable", 0, 0, font, Color.DARK_GRAY);
      }

      if (false) // add the legend
        mi.combineImagesVertical(mi.getImage(),color.getLegendImage(col, 60, 28, "Test Var", ""));

      //output to HDFS
      Configuration conf = new Configuration();
      //conf.set("fs.defaultFS", MyProperty.nameNode);
      conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
      conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
      conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
      FileSystem fs=null;
      try {
        fs = FileSystem.get(conf);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

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

}
