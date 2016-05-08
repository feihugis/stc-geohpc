package edu.gmu.stc.spark.io.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.joda.time.DateTime;
import org.joda.time.Months;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import edu.gmu.stc.configure.MyProperty;
import edu.gmu.stc.datavisualization.taylordiagram.TaylorDiagramFactory;
import edu.gmu.stc.datavisualization.taylordiagram.TaylorDiagramUnit;
import edu.gmu.stc.hadoop.index.io.merra.NcHdfsRaf;
import edu.gmu.stc.hadoop.raster.DataChunk;
import edu.gmu.stc.hadoop.raster.DataChunkInputFormat;
import edu.gmu.stc.hadoop.raster.hdf5.ArrayFloatSerializer;
import edu.gmu.stc.spark.io.kryo.SparkKryoRegistrator;
import scala.Tuple2;
import scala.tools.nsc.transform.patmat.ScalaLogic;
import ucar.ma2.ArrayFloat;
import ucar.ma2.Index3D;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

/**
 * Created by Fei Hu on 4/30/16.
 */
public class TaylorDiagranAnalysis {

  public static String[] getCorner(float lat_min, float lat_max, float lon_min, float lon_max, int[] shape) {
      String[] corner = new String[3];
      corner[0] = "0";
      float lat_unit = 180.00f/shape[1];
      float lon_unit = 360.00f/shape[2];
      corner[1] = ((int) ((90.00f + lat_min)/lat_unit)) + "";
      corner[2] = ((int) ((180.00f + lon_min)/lon_unit)) + "";
      return corner;
  }

  public static String[] getShape(float lat_min, float lat_max, float lon_min, float lon_max, int[] shape) {
    String[] result = new String[3];
    result[0] = "1";
    float lat_unit = 180.00f/shape[1];
    float lon_unit = 360.00f/shape[2];
    result[1] = ((int) ((lat_max - lat_min)/lat_unit)) + "";
    result[2] = ((int) ((lon_max - lon_min)/lon_unit)) + "";
    return result;
  }

  public static TaylorDiagramUnit cfsrmean(FileSystem fs, String inputPath,
                                           float lat_min, float lat_max, float lon_min, float lon_max,
                                           int startTime, int endTime) throws IOException,
                                                                              InvalidRangeException,
                                                                              ParseException {
    Path path = new Path(inputPath);
    NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(path), fs.getConf());
    NetcdfFile nc = NetcdfFile.open(raf, path.toString());
    int[] corner = new int[3];
    int[] shape = new int[3];

    SimpleDateFormat originalFormat = new SimpleDateFormat("yyyyMM");
    DateTime startDate = new DateTime().withDate(startTime/100, startTime%100, 1);
    DateTime endDate = new DateTime().withDate(endTime/100, endTime%100, 1);
    DateTime corner_beginDate = new DateTime().withDate(1979, 1, 1);

    Variable var = nc.findVariable("Total_precipitation_surface_6_Hour_AverageForecastAccumulations-204");
    ArrayFloat latArray = (ArrayFloat) nc.findVariable("lat").read();
    ArrayFloat lonArray = (ArrayFloat) nc.findVariable("lon").read();

    Months corner_month = Months.monthsBetween(corner_beginDate, startDate);
    Months shape_month = Months.monthsBetween(startDate, endDate);
    corner[0] = corner_month.getMonths()+1;
    shape[0] = shape_month.getMonths()+1;
    ArrayFloat valueArray = (ArrayFloat) var.read(new int[]{corner[0], 0, 0}, new int[]{shape[0], 361, 720});

    Float[] means = new Float[shape[0]];

    for (int time=0; time<shape[0]; time++) {
      float sum = 0.0f;
      int count = 0;
      for (int i=0; i<361; i++) {
        for (int j=0; j<720; j++) {
          float lat = latArray.getFloat(i);
          float lon = lonArray.getFloat(j);
          if (lon>180.0) {
            lon = lon - 360;
          }
          if (lon<=lon_max && lon>=lon_min && lat<=lat_max && lat>=lat_min) {
             float v = valueArray.getFloat(time*361*720 + i*720 + j);
             if (v<10000 && v>0) {
               sum = sum + v;
               count++;
             }

          }
        }
      }
      means[time] = sum/count;
    }

    return new TaylorDiagramUnit("CFSR", means);
  }

  public static TaylorDiagramUnit cmapmean(FileSystem fs, String inputPath,
                                           float lat_min, float lat_max, float lon_min, float lon_max,
                                           int startTime, int endTime) throws IOException, InvalidRangeException {
    Path path = new Path(inputPath);
    NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(path), fs.getConf());
    NetcdfFile nc = NetcdfFile.open(raf, path.toString());
    Variable var = nc.findVariable("precip");
    ArrayFloat latArray = (ArrayFloat) nc.findVariable("lat").read();
    ArrayFloat lonArray = (ArrayFloat) nc.findVariable("lon").read();

    int[] corner = new int[3];
    int[] shape = new int[3];

    DateTime startDate = new DateTime().withDate(startTime/100, startTime%100, 1);
    DateTime endDate = new DateTime().withDate(endTime/100, endTime%100, 1);
    DateTime corner_beginDate = new DateTime().withDate(1979, 1, 1);

    Months corner_month = Months.monthsBetween(corner_beginDate, startDate);
    Months shape_month = Months.monthsBetween(startDate, endDate);
    corner[0] = corner_month.getMonths()+1;
    shape[0] = shape_month.getMonths()+1;
    ArrayFloat valueArray = (ArrayFloat) var.read(new int[]{corner[0], 0, 0}, new int[]{shape[0], 72, 144});

    Float[] means = new Float[shape[0]];

    for (int time=0; time<shape[0]; time++) {
      float sum = 0.0f;
      int count = 0;
      for (int i=0; i<72; i++) {
        for (int j=0; j<144; j++) {
          float lat = latArray.getFloat(i);
          float lon = lonArray.getFloat(j);
          if (lon>180.0) {
            lon = lon - 360;
          }
          if (lon<=lon_max && lon>=lon_min && lat<=lat_max && lat>=lat_min) {
            float v = valueArray.getFloat(time*72*144 + i*144 + j);
            if (v<10000 && v>0) {
              sum = sum + v;
              count++;
            }

          }
        }
      }
      means[time] = sum/count;
    }

    return new TaylorDiagramUnit("CMAP", means);
  }

  public static TaylorDiagramUnit gpcpmean(FileSystem fs, String inputPath,
                                           float lat_min, float lat_max, float lon_min, float lon_max,
                                           int startTime, int endTime) throws IOException, InvalidRangeException {
    Path path = new Path(inputPath);
    NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(path), fs.getConf());
    NetcdfFile nc = NetcdfFile.open(raf, path.toString());
    Variable var = nc.findVariable("precip");
    ArrayFloat latArray = (ArrayFloat) nc.findVariable("lat").read();
    ArrayFloat lonArray = (ArrayFloat) nc.findVariable("lon").read();

    int[] corner = new int[3];
    int[] shape = new int[3];

    DateTime startDate = new DateTime().withDate(startTime/100, startTime%100, 1);
    DateTime endDate = new DateTime().withDate(endTime/100, endTime%100, 1);
    DateTime corner_beginDate = new DateTime().withDate(1979, 1, 1);

    Months corner_month = Months.monthsBetween(corner_beginDate, startDate);
    Months shape_month = Months.monthsBetween(startDate, endDate);
    corner[0] = corner_month.getMonths()+1;
    shape[0] = shape_month.getMonths()+1;
    ArrayFloat valueArray = (ArrayFloat) var.read(new int[]{corner[0], 0, 0}, new int[]{shape[0], 72, 144});

    Float[] means = new Float[shape[0]];

    for (int time=0; time<shape[0]; time++) {
      float sum = 0.0f;
      int count = 0;
      for (int i=0; i<72; i++) {
        for (int j=0; j<144; j++) {
          float lat = latArray.getFloat(i);
          float lon = lonArray.getFloat(j);
          if (lon>180.0) {
            lon = lon - 360;
          }
          if (lon<=lon_max && lon>=lon_min && lat<=lat_max && lat>=lat_min) {
            float v = valueArray.getFloat(time*72*144 + i*144 + j);
            if (v<10000 && v>0) {
              sum = sum + v;
              count++;
            }

          }
        }
      }
      means[time] = sum/count;
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
      throws ClassNotFoundException, IOException, InvalidRangeException, InterruptedException,
             ParseException {

    int startTime = Integer.parseInt("198001");
    int endTime = Integer.parseInt("198012");
    float lat_min = Float.parseFloat("-90.00");
    float lat_max = Float.parseFloat("90.00");
    float lon_min = Float.parseFloat("-180.00");
    float lon_max = Float.parseFloat("180.00");

    int[] MERRA2_SHAPE = new int[]{1, 361, 576};
    int[] MERRA1_SHAPE = new int[]{1, 361, 540};
    int[] ERA_INTRIM_SHAPE = new int[]{1, 256, 512};

    final SparkConf sconf = new SparkConf().setAppName("SparkTest").setMaster("local[6]");
    sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sconf.set("spark.kryo.registrator", SparkKryoRegistrator.class.getName());

    JavaSparkContext sc = new JavaSparkContext(sconf);

    Configuration hconf = new Configuration();
    hconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    hconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    hconf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
    hconf.set("fs.defaultFS", MyProperty.nameNode);
    hconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    hconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    hconf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
    FileSystem fs = FileSystem.get(hconf);

    /*for merra-2 datasets*/
    String merra2Path = "/Users/feihu/Documents/Data/Merra2/monthly";
    Configuration merra2Conf = new Configuration();
    merra2Conf.set("dataType", "float");
    merra2Conf.setStrings("fileFormats", new String[]{"nc4"});
    merra2Conf.setStrings("variables", new String[]{"PRECTOT"});
    merra2Conf.set("mapreduce.input.fileinputformat.inputdir", merra2Path);
    merra2Conf.setStrings("corner", TaylorDiagranAnalysis.getCorner(lat_min,lat_max, lon_min, lon_max, MERRA2_SHAPE));
    merra2Conf.setStrings("shape", TaylorDiagranAnalysis.getShape(lat_min,lat_max, lon_min, lon_max, MERRA2_SHAPE));
    merra2Conf.setStrings("dimension", new String[]{"time", "lat", "lon"});
    merra2Conf.setInt("startDate", startTime);
    merra2Conf.setInt("endDate", endTime);

    JavaPairRDD<DataChunk, ArrayFloatSerializer> merra2Records = sc.newAPIHadoopRDD(merra2Conf, DataChunkInputFormat.class,
                                                                                    DataChunk.class,
                                                                                    ArrayFloatSerializer.class);
    JavaPairRDD<String, Tuple2<Float,Long>> merra2SumCount = merra2Records.mapToPair(
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

    JavaPairRDD<String, Float> merra2MeanRDD = merra2SumCount.mapValues(
        new Function<Tuple2<Float, Long>, Float>() {
          @Override
          public Float call(Tuple2<Float, Long> v1) throws Exception {
            return v1._1()/v1._2()*3600*24;
          }
        }).sortByKey(true);

    List<Float> merra2MeanList = merra2MeanRDD.values().collect();
    Float[] merra2MeanArray = new Float[merra2MeanList.size()];
    merra2MeanList.toArray(merra2MeanArray);

    TaylorDiagramUnit merra2Unit = new TaylorDiagramUnit("MERRA-2", merra2MeanArray);
    TaylorDiagramUnit cfsrUnit = TaylorDiagranAnalysis.cfsrmean(fs, "/Users/feihu/Documents/Data/CFSR/pgbh06.gdas.A_PCP.SFC.grb2",
                                                                lat_min, lat_max, lon_min, lon_max, startTime, endTime);
    TaylorDiagramUnit cmapUnit = TaylorDiagranAnalysis.cmapmean(fs, "/Users/feihu/Documents/Data/CMAP/precip.mon.mean.standard.nc",
                                                                lat_min, lat_max, lon_min, lon_max, startTime, endTime);
    TaylorDiagramUnit gpcpUnit = TaylorDiagranAnalysis.gpcpmean(fs, "/Users/feihu/Documents/Data/GPCPV2.2/precip.mon.mean.nc",
                                                                lat_min, lat_max, lon_min, lon_max, startTime, endTime);

    float[] yFloat = gpcpUnit.getValues();
    double[] yArray = new double[yFloat.length];
    for (int i=0; i<yArray.length; i++) {
      yArray[i] = yFloat[i];
    }

    /*for ERA-Intrim datasets*/
    String erainterimPath = "/Users/feihu/Documents/Data/era-interim/";
    Configuration eraIntrimConf = new Configuration();
    eraIntrimConf.set("dataType", "float");
    eraIntrimConf.setStrings("fileFormats", new String[]{"00"});
    eraIntrimConf.setStrings("variables", new String[]{"Total_precipitation_surface_12_Hour_120"});
    eraIntrimConf.set("mapreduce.input.fileinputformat.inputdir", erainterimPath);
    eraIntrimConf.setStrings("corner", TaylorDiagranAnalysis.getCorner(lat_min,lat_max, lon_min, lon_max, ERA_INTRIM_SHAPE));
    eraIntrimConf.setStrings("shape", TaylorDiagranAnalysis.getShape(lat_min,lat_max, lon_min, lon_max, ERA_INTRIM_SHAPE));
    eraIntrimConf.setStrings("dimension", new String[]{"time", "lat", "lon"});
    eraIntrimConf.setInt("startDate", startTime*10000+100);
    eraIntrimConf.setInt("endDate", endTime*10000+100);

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

    /*for merra-1 datasets*/
    String merra1Path = "/Users/feihu/Documents/Data/Merra/";
    RemoteIterator<LocatedFileStatus> merra1fileStatList = fs.listFiles(new Path(erainterimPath), true);
    Configuration merra1Conf = new Configuration();
    merra1Conf.set("dataType", "float");
    merra1Conf.setStrings("fileFormats", new String[]{"hdf"});
    merra1Conf.setStrings("variables", new String[]{"PRECTOT"});
    merra1Conf.set("mapreduce.input.fileinputformat.inputdir", merra1Path);
    merra1Conf.setStrings("corner", TaylorDiagranAnalysis.getCorner(lat_min,lat_max, lon_min, lon_max, MERRA1_SHAPE));
    merra1Conf.setStrings("shape", TaylorDiagranAnalysis.getShape(lat_min,lat_max, lon_min, lon_max, MERRA1_SHAPE));
    merra1Conf.setStrings("dimension", new String[]{"time", "lat", "lon"});
    merra1Conf.setInt("startDate", startTime);
    merra1Conf.setInt("endDate", endTime);

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
