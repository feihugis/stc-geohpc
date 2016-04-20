package edu.gmu.stc.spark.test.query.merra;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.configure.MyProperty;
import edu.gmu.stc.hadoop.index.io.merra.NcHdfsRaf;
import ucar.ma2.ArrayFloat;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

/**
 * Created by Fei Hu on 4/12/16.
 */
public class GridOneTest {
  FileSystem fs = null;
  FSDataInputStream fsInput = null;
  Configuration conf = new Configuration();

  public GridOneTest() throws IOException {
    conf.set("fs.defaultFS", MyProperty.nameNode);
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
    fs = FileSystem.get(conf);
  }

  public void merra1mean() throws IOException {
    //Path path = new Path("/Users/feihu/Documents/Data/Merra/MERRA100.prod.simul.tavgM_2d_mld_Nx.198001.hdf");
    List<Path> fileList = new ArrayList<Path>();
    for (int i = 1; i<13; i++) {
      if (i<10) {
        fileList.add(new Path("/Users/feihu/Documents/Data/Merra/MERRA100.prod.simul.tavgM_2d_mld_Nx.19800"+i+".hdf"));
      } else {
        fileList.add(new Path("/Users/feihu/Documents/Data/Merra/MERRA100.prod.simul.tavgM_2d_mld_Nx.1980"+i+".hdf"));
      }
    }

    int count = 0;
    float sum = 0.0f;

    for (Path path : fileList) {
      NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(path), fs.getConf());
      NetcdfFile nc = NetcdfFile.open(raf, path.toString());
      List<Variable> variables = nc.getVariables();
      for (Variable var: variables) {
        if (var.getShortName().equals("PRECTOT")) {
          Attribute at = (Attribute) var.getAttributes().get(0);
          float fillValue = (float) at.getNumericValue();
          ArrayFloat values = (ArrayFloat) var.read();

          for (int i=0; i<values.getSize(); i++) {
            float v = values.getFloat(i);
            if (v != fillValue) {
              sum = sum + v;
              count++;
            }
          }
        }
      }
    }

    System.out.println("**********   Merra count " + count + "; mean value = " + sum/count*24*3600);

  }

  public void merra2mean() throws IOException, InvalidRangeException {
    List<Path> fileList = new ArrayList<Path>();
    for (int i = 1; i<2; i++) {
      if (i<10) {
        fileList.add(new Path("/Users/feihu/Documents/Data/Merra2/monthly/MERRA2_100.tavgM_2d_flx_Nx.19800"+i+".nc4"));
      } else {
        fileList.add(new Path("/Users/feihu/Documents/Data/Merra2/monthly/MERRA2_100.tavgM_2d_flx_Nx.1980"+i+".nc4"));
      }
    }

    int count = 0;
    float sum = 0.0f;

    for (Path path : fileList) {
      NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(path), fs.getConf());
      NetcdfFile nc = NetcdfFile.open(raf, path.toString());
      List<Variable> variables = nc.getVariables();
      int test_count = 0;
      float test_sum = 0.0f;
      for (Variable var: variables) {
        if (var.getShortName().equals("PRECTOT")) {
          ArrayFloat values = (ArrayFloat) var.read();
          int[] shape = values.getShape();
          for (int i=0; i<values.getSize(); i++) {
            float v = values.getFloat(i);
            if (v <1000 && v>0) {
              sum = sum + v;
              count++;
              test_sum = test_sum+v;
              test_count = test_count+1;
            }
          }
        }
      }
    }
    System.out.println("**********   Merra2 count " + count + "; mean value = " + sum/count*24*3600 );
  }

  public void eraintrimmean() throws IOException {
    List<Path> fileList = new ArrayList<Path>();
    for (int i = 1; i<13; i++) {
      if (i<10) {
        fileList.add(new Path("/Users/feihu/Documents/Data/era-interim/ei.mdfa.fc12hr.sfc.regn128sc.19800"+i+"0100"));
      } else {
        fileList.add(new Path("/Users/feihu/Documents/Data/era-interim/ei.mdfa.fc12hr.sfc.regn128sc.1980"+i+"0100"));
      }
    }

    int count = 0;
    float sum = 0.0f;

    for (Path path : fileList) {
      NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(path), fs.getConf());
      NetcdfFile nc = NetcdfFile.open(raf, path.toString());
      List<Variable> variables = nc.getVariables();
      for (Variable var: variables) {
        if (var.getShortName().equals("Total_precipitation_surface_12_Hour_120")) {
          ArrayFloat values = (ArrayFloat) var.read();
          for (int i=0; i<values.getSize(); i++) {
            float v = values.getFloat(i);
            if (v <1000 && v>0) {
              sum = sum + v;
              count++;
            }
          }
        }
      }
    }
    System.out.println("**********   ERA-Intrim count " + count + "; mean value = " + sum/count*1000);
  }

  public void cfsrmean() throws IOException, InvalidRangeException {
    Path path = new Path("/Users/feihu/Documents/Data/CFSR/pgbh06.gdas.A_PCP.SFC.grb2");
    NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(path), fs.getConf());
    NetcdfFile nc = NetcdfFile.open(raf, path.toString());
    List<Variable> variables = nc.getVariables();
    int count = 0;
    float sum = 0.0f;

    for (Variable var: variables) {
      System.out.println(var.getDescription());
      if (var.getShortName().contains("Total_precipitation")) {
        ArrayFloat values = (ArrayFloat) var.read(new int[]{0,0,0}, new int[]{12,361,720});
        for (int i=0; i<values.getSize(); i++) {
          float v = values.getFloat(i);
          if (v <1000 && v>0) {
            sum = sum + v;
            count++;
          }
        }
      }
    }

    System.out.println("**********   CFSR count " + count + "; mean value = " + sum/count*4);
  }

  public void cmapmean() throws IOException, InvalidRangeException {
    Path path = new Path("/Users/feihu/Documents/Data/CMAP/precip.mon.mean.standard.nc");
    NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(path), fs.getConf());
    NetcdfFile nc = NetcdfFile.open(raf, path.toString());
    List<Variable> variables = nc.getVariables();
    for (Variable var: variables) {
      if (var.getShortName().equals("precip")) {
        ArrayFloat values = (ArrayFloat) var.read(new int[]{12,0,0}, new int[]{23, 72, 144});
        int count = 0;
        float sum = 0.0f;
        for (int i=0; i<values.getSize(); i++) {
          float v = values.getFloat(i);
          if (v < 1000 && v > 0) {
            sum = sum + v;
            count++;
          }
        }
        System.out.println("**********   CMAP count " + count + "; mean value = " + sum/count);
      }
    }
  }

  public void gpcpmean() throws IOException, InvalidRangeException {
    Path path = new Path("/Users/feihu/Documents/Data/GPCPV2.2/precip.mon.mean.nc");
    NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(path), fs.getConf());
    NetcdfFile nc = NetcdfFile.open(raf, path.toString());
    List<Variable> variables = nc.getVariables();
    for (Variable var: variables) {
      if (var.getShortName().equals("precip")) {
        ArrayFloat values = (ArrayFloat) var.read(new int[]{12,0,0}, new int[]{23, 72, 144});
        int count = 0;
        float sum = 0.0f;
        for (int i=0; i<values.getSize(); i++) {
          float v = values.getFloat(i);
          if (v < 1000 && v > 0) {
            sum = sum + v;
            count++;
          }
        }

        System.out.println("**********   GPCP count " + count + "; GPCP value = " + sum/count);
      }
    }
  }

  public static void main(String[] args) throws IOException, InvalidRangeException {
    GridOneTest test = new GridOneTest();
    test.merra1mean();
    test.merra2mean();
    test.eraintrimmean();
    //test.cfsrmean();
    //test.cmapmean();
    //test.gpcpmean();

  }
}
