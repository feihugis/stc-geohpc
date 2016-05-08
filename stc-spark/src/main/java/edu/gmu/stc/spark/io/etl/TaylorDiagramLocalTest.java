package edu.gmu.stc.spark.io.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.gmu.stc.configure.MyProperty;
import edu.gmu.stc.datavisualization.taylordiagram.TaylorDiagramFactory;
import edu.gmu.stc.datavisualization.taylordiagram.TaylorDiagramUnit;
import edu.gmu.stc.hadoop.index.io.merra.NcHdfsRaf;
import edu.gmu.stc.hadoop.raster.DataChunk;
import edu.gmu.stc.hadoop.raster.DataChunkInputFormat;
import edu.gmu.stc.hadoop.raster.grib1.Grib1Chunk;
import edu.gmu.stc.hadoop.raster.hdf5.ArrayFloatSerializer;
import edu.gmu.stc.hadoop.raster.hdf5.H5FileInputFormat;
import edu.gmu.stc.hadoop.raster.index.MetaData;
import edu.gmu.stc.hadoop.vector.Rectangle;
import edu.gmu.stc.spark.io.kryo.SparkKryoRegistrator;
import scala.Tuple2;
import scala.math.Ordering;
import ucar.ma2.ArrayFloat;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

/**
 * Created by Fei Hu on 4/18/16.
 */
public class TaylorDiagramLocalTest {

  public void eraintrimmean(FileSystem fs) throws IOException {
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

  public static TaylorDiagramUnit cfsrmean(FileSystem fs, String inputPath) throws IOException, InvalidRangeException {
    Path path = new Path(inputPath);
    NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(path), fs.getConf());
    NetcdfFile nc = NetcdfFile.open(raf, path.toString());
    List<Variable> variables = nc.getVariables();

    Float[] means = new Float[12];
    for (Variable var: variables) {
      System.out.println(var.getDescription());
      if (var.getShortName().contains("Total_precipitation")) {
        for (int i=0; i<12; i++) {
          int count = 0;
          float sum = 0.0f;
          ArrayFloat values = (ArrayFloat) var.read(new int[]{i,0,0}, new int[]{1,361,720});
          for (int j=0; j<values.getSize(); j++) {
            float v = values.getFloat(j);
            if (v <1000 && v>0) {
              sum = sum + v;
              count++;
            }
          }
          means[i] = sum/count;
        }
      }
    }

    return new TaylorDiagramUnit("CFSR", means);
  }

  public static TaylorDiagramUnit cmapmean(FileSystem fs, String inputPath) throws IOException, InvalidRangeException {
    Path path = new Path(inputPath);
    NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(path), fs.getConf());
    NetcdfFile nc = NetcdfFile.open(raf, path.toString());
    List<Variable> variables = nc.getVariables();

    Float[] means = new Float[12];
    for (Variable var: variables) {
      if (var.getShortName().equals("precip")) {
        for (int i=12; i<24; i++) {
          int count = 0;
          float sum = 0.0f;
          ArrayFloat values = (ArrayFloat) var.read(new int[]{i,0,0}, new int[]{1,72,144});
          for (int j=0; j<values.getSize(); j++) {
            float v = values.getFloat(j);
            if (v <1000 && v>0) {
              sum = sum + v;
              count++;
            }
          }
          means[i-12] = sum/count;
        }
      }
    }

    return new TaylorDiagramUnit("CMAP", means);
  }

  public static TaylorDiagramUnit gpcpmean(FileSystem fs, String inputPath) throws IOException, InvalidRangeException {
    Path path = new Path(inputPath);
    NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(path), fs.getConf());
    NetcdfFile nc = NetcdfFile.open(raf, path.toString());
    List<Variable> variables = nc.getVariables();

    Float[] means = new Float[12];
    for (Variable var: variables) {
      if (var.getShortName().equals("precip")) {
        for (int i = 12; i < 24; i++) {
          int count = 0;
          float sum = 0.0f;
          ArrayFloat values = (ArrayFloat) var.read(new int[]{i, 0, 0}, new int[]{1, 72, 144});
          for (int j = 0; j < values.getSize(); j++) {
            float v = values.getFloat(j);
            if (v < 1000 && v > 0) {
              sum = sum + v;
              count++;
            }
          }
          means[i-12] = sum / count;
        }
      }
    }
    return new TaylorDiagramUnit("GPCP", means);
  }

  /**
   *
   * @param args
   * @throws ClassNotFoundException
   * @throws IOException
   * @throws InvalidRangeException
   * @throws InterruptedException
   */
  public static void main(final String[] args)
      throws ClassNotFoundException, IOException, InvalidRangeException, InterruptedException {

    if (args.length != 9) {
      System.out.println(
          "Please input 9 input parameters, <vars> <inputdir> <bbox> <startTime> <endTime> <jsonPath> <resultPath> <states> <isQueryorExcludedstates>");
      return;
    }

    final SparkConf sconf = new SparkConf().setAppName("SparkTest").setMaster("local[6]");
    sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sconf.set("spark.kryo.registrator", SparkKryoRegistrator.class.getName());

    JavaSparkContext sc = new JavaSparkContext(sconf);
    Configuration hconf = new Configuration();
    hconf.setStrings("variables", args[0]); //"LWTNET,UFLXKE";//"UFLXKE,AUTCNVRN,BKGERR";
    hconf.set("mapreduce.input.fileinputformat.inputdir", args[1]);
    hconf.setStrings("bbox", args[2]); //"[0-1,0-361,0-576],[5-6,0-361,0-576]"
    hconf.setStrings("startTime", args[3]);  //"19800101"
    hconf.setStrings("endTime", args[4]); //"20151201"
    hconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    hconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    hconf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

    hconf.set("fs.defaultFS", MyProperty.nameNode);
    hconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    hconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    hconf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
    FileSystem fs = FileSystem.get(hconf);

    Rectangle queryBBox = new Rectangle(-190.0, -99.0, 199.0, 99.0);
    hconf.set("geoBBox", queryBBox.toWKT());

    JavaPairRDD<DataChunk, ArrayFloatSerializer> merra2Records = sc.newAPIHadoopRDD(hconf, H5FileInputFormat.class,
                                                                                    DataChunk.class,
                                                                                    ArrayFloatSerializer.class);

    JavaPairRDD<String, Tuple2<Float, Long>> merra2SumCountRDD = merra2Records.mapValues(
        new Function<ArrayFloatSerializer, Tuple2<Float, Long>>() {
          @Override
          public Tuple2<Float, Long> call(ArrayFloatSerializer v1) throws Exception {
            ArrayFloat values = v1.getArray();
            long count = 0;
            float sum = 0.0f;
            for (int i=0; i<values.getSize(); i++) {
             float v = values.getFloat(i);
             if (v<1000 && v>0) {
               sum = sum + v;
               count++;
             }
            }
            return new Tuple2<Float, Long>(sum, count);
          }
        }).mapToPair(
        new PairFunction<Tuple2<DataChunk, Tuple2<Float, Long>>, String, Tuple2<Float, Long>>() {
          @Override
          public Tuple2<String, Tuple2<Float, Long>> call(
              Tuple2<DataChunk, Tuple2<Float, Long>> tuple2) throws Exception {
            String key = tuple2._1.getVarShortName() + "_" + tuple2._1().getFilePath().split("\\.")[2] + "_" + tuple2._1().getCorner()[0];
            return new Tuple2<String, Tuple2<Float, Long>>(key, tuple2._2());
          }
        }).reduceByKey(
        new Function2<Tuple2<Float, Long>, Tuple2<Float, Long>, Tuple2<Float, Long>>() {
          @Override
          public Tuple2<Float, Long> call(Tuple2<Float, Long> v1, Tuple2<Float, Long> v2)
              throws Exception {
            float sum = v1._1() + v2._1();
            Long count = v1._2() + v2._2();
            return new Tuple2<Float, Long>(sum, count);
          }
        });

    JavaPairRDD<String, Float> merra2MeanRDD = merra2SumCountRDD.mapValues(
        new Function<Tuple2<Float, Long>, Float>() {
          @Override
          public Float call(Tuple2<Float, Long> v1) throws Exception {
            return new Float(v1._1()/v1._2()*24*3600);
          }
        }).sortByKey(true);

    List<Float> merra2MeanList = merra2MeanRDD.values().collect();
    Float[] merra2MeanArray = new Float[merra2MeanList.size()];
    merra2MeanList.toArray(merra2MeanArray);

    TaylorDiagramUnit merra2Unit = new TaylorDiagramUnit("MERRA2", merra2MeanArray);
    TaylorDiagramUnit cfsrUnit = TaylorDiagramLocalTest.cfsrmean(fs, "/Users/feihu/Documents/Data/CFSR/pgbh06.gdas.A_PCP.SFC.grb2");
    TaylorDiagramUnit cmapUnit = TaylorDiagramLocalTest.cmapmean(fs, "/Users/feihu/Documents/Data/CMAP/precip.mon.mean.standard.nc");
    TaylorDiagramUnit gpcpUnit = TaylorDiagramLocalTest.gpcpmean(fs, "/Users/feihu/Documents/Data/GPCPV2.2/precip.mon.mean.nc");

    float[] yFloat = gpcpUnit.getValues();
    double[] yArray = new double[yFloat.length];
    for (int i=0; i<yArray.length; i++) {
      yArray[i] = yFloat[i];
    }



    /*for ERA-Intrim datasets*/
    String erainterimPath = "/Users/feihu/Documents/Data/era-interim/";
    RemoteIterator<LocatedFileStatus> fileStatList = fs.listFiles(new Path(erainterimPath), true);
    Configuration eraIntrimConf = new Configuration();
    eraIntrimConf.set("dataType", "float");
    eraIntrimConf.setStrings("fileFormats", new String[]{"00"});
    eraIntrimConf.setStrings("variables", new String[]{"Total_precipitation_surface_12_Hour_120"});
    eraIntrimConf.set("mapreduce.input.fileinputformat.inputdir", erainterimPath);
    eraIntrimConf.setStrings("corner", new String[]{"0","0","0"});
    eraIntrimConf.setStrings("shape", new String[]{"1","256","512"});
    eraIntrimConf.setStrings("dimension", new String[]{"time", "lat", "lon"});

    JavaPairRDD<DataChunk, ArrayFloatSerializer> eraIntrimRecords = sc.newAPIHadoopRDD(eraIntrimConf, DataChunkInputFormat.class,
                                                                                    DataChunk.class,
                                                                                    ArrayFloatSerializer.class);
    JavaPairRDD<String, Tuple2<Float,Long>> eraIntrimSumCount = eraIntrimRecords.mapToPair(
        new PairFunction<Tuple2<DataChunk, ArrayFloatSerializer>, String, Tuple2<Float, Long>>() {
          @Override
          public Tuple2<String, Tuple2<Float, Long>> call(Tuple2<DataChunk, ArrayFloatSerializer> tuple2) throws Exception {
            ArrayFloat arrayFloat = tuple2._2().getArray();
            Float sum = 0.0f;
            Long count = 0L;
            for (int i=0; i<arrayFloat.getSize(); i++) {
              float v = arrayFloat.getFloat(i);
              if (v>0 && v<1000) {
                sum = sum + v;
                count++;
              }
            }
            return new Tuple2<String, Tuple2<Float, Long>>(tuple2._1().getFilePath()+"_"+tuple2._1().getVarShortName(), new Tuple2<Float, Long>(sum, count));
          }
        });

    JavaPairRDD<String, Float> eraIntrimMeanRDD = eraIntrimSumCount.mapValues(
        new Function<Tuple2<Float, Long>, Float>() {
          @Override
          public Float call(Tuple2<Float, Long> v1) throws Exception {
            return v1._1()/v1._2()*1000;
          }
        }).sortByKey(true);

    List<Float> eraIntrimMeanList = eraIntrimMeanRDD.values().collect();
    Float[] eraIntrimMeanArray = new Float[eraIntrimMeanList.size()];
    eraIntrimMeanList.toArray(eraIntrimMeanArray);
    TaylorDiagramUnit eraIntrimUnit = new TaylorDiagramUnit("ERA-INTRIM", eraIntrimMeanArray);

    merra2Unit.calSTDAndPersonsCorrelation(yArray);
    cfsrUnit.calSTDAndPersonsCorrelation(yArray);
    cmapUnit.calSTDAndPersonsCorrelation(yArray);
    gpcpUnit.calSTDAndPersonsCorrelation(yArray);
    eraIntrimUnit.calSTDAndPersonsCorrelation(yArray);



    /*for merra-1 datasets*/
    String merra1Path = "/Users/feihu/Documents/Data/Merra/";
    RemoteIterator<LocatedFileStatus> merra1fileStatList = fs.listFiles(new Path(erainterimPath), true);
    Configuration merra1Conf = new Configuration();
    merra1Conf.set("dataType", "float");
    merra1Conf.setStrings("fileFormats", new String[]{"hdf"});
    merra1Conf.setStrings("variables", new String[]{"PRECTOT"});
    merra1Conf.set("mapreduce.input.fileinputformat.inputdir", merra1Path);
    merra1Conf.setStrings("corner", new String[]{"0","0","0"});
    merra1Conf.setStrings("shape", new String[]{"1","361","540"});
    merra1Conf.setStrings("dimension", new String[]{"time", "lat", "lon"});
    merra1Conf.setInt("startDate", 198001);
    merra1Conf.setInt("endDate", 198012);

    JavaPairRDD<DataChunk, ArrayFloatSerializer> merra1Records = sc.newAPIHadoopRDD(merra1Conf, DataChunkInputFormat.class,
                                                                                       DataChunk.class,
                                                                                       ArrayFloatSerializer.class);
    JavaPairRDD<String, Tuple2<Float,Long>> merra1SumCount = merra1Records.mapToPair(
        new PairFunction<Tuple2<DataChunk, ArrayFloatSerializer>, String, Tuple2<Float, Long>>() {
          @Override
          public Tuple2<String, Tuple2<Float, Long>> call(Tuple2<DataChunk, ArrayFloatSerializer> tuple2) throws Exception {
            ArrayFloat arrayFloat = tuple2._2().getArray();

            Float sum = 0.0f;
            Long count = 0L;
            for (int i=0; i<arrayFloat.getSize(); i++) {
              float v = arrayFloat.getFloat(i);
              if (v>0 && v<1000) {
                sum = sum + v;
                count++;
              }
            }
            return new Tuple2<String, Tuple2<Float, Long>>(tuple2._1().getFilePath()+"_"+tuple2._1().getVarShortName(), new Tuple2<Float, Long>(sum, count));
          }
        });

    JavaPairRDD<String, Float> merra1MeanRDD = merra1SumCount.mapValues(
        new Function<Tuple2<Float, Long>, Float>() {
          @Override
          public Float call(Tuple2<Float, Long> v1) throws Exception {
            return v1._1()/v1._2()*3600*24;
          }
        }).sortByKey(true);

    List<Float> merra1MeanList = merra1MeanRDD.values().collect();
    Float[] merra1MeanArray = new Float[merra1MeanList.size()];
    merra1MeanList.toArray(merra1MeanArray);
    TaylorDiagramUnit merra1Unit = new TaylorDiagramUnit("MERRA-1", merra1MeanArray);

    merra2Unit.calSTDAndPersonsCorrelation(yArray);
    cfsrUnit.calSTDAndPersonsCorrelation(yArray);
    cmapUnit.calSTDAndPersonsCorrelation(yArray);
    gpcpUnit.calSTDAndPersonsCorrelation(yArray);
    eraIntrimUnit.calSTDAndPersonsCorrelation(yArray);
    merra1Unit.calSTDAndPersonsCorrelation(yArray);

    List<TaylorDiagramUnit> taylorDiagramUnits = new ArrayList<TaylorDiagramUnit>();

    //should add the reference unit first
    taylorDiagramUnits.add(cmapUnit);
    taylorDiagramUnits.add(gpcpUnit);
    taylorDiagramUnits.add(merra2Unit);
    taylorDiagramUnits.add(cfsrUnit);
    taylorDiagramUnits.add(eraIntrimUnit);
    taylorDiagramUnits.add(merra1Unit);

    String json_output = "/Users/feihu/Desktop/taylordiagram.json";
    String tydPythonSript = "/Users/feihu/Documents/GitHub/stc-geohpc/stc-datavisualization/src/main/java/edu/gmu/stc/datavisualization/python/TaylorDiagram.py"; //"/Users/feihu/Desktop/taylordiagram-v2.py";
    String x_axis = "Precipitation";
    String y_axis = "values(mm/day)";
    String tyd_output = "/Users/feihu/Desktop/taylordiagram-reanalysis.png";
    TaylorDiagramFactory.savetoJSON(taylorDiagramUnits, json_output);
    TaylorDiagramFactory.drawTaylorDiagram(tydPythonSript, json_output, x_axis, y_axis, tyd_output);

  }
}
