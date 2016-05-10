package edu.gmu.stc.spark.io.etl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.spark.sql.columnar.BOOLEAN;
import org.joda.time.DateTime;
import org.joda.time.Months;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import ucar.ma2.ArrayFloat;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

/**
 * Created by Fei Hu on 4/30/16.
 */

public class TaylorDiagramAnalysis {
  private static final Log LOG = LogFactory.getLog(TaylorDiagramAnalysis.class);

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

  public static TaylorDiagramUnit generateTaylorDiagramUnit(JavaSparkContext sc, String unitName, String dataPath, String[] formats, String[] vars,
                                                            float maxValue, float minValue,
                                                            float lat_min, float lat_max, float lon_min, float lon_max, int[] shape,
                                                            int startTime, int endTime,
                                                            int unitScale) {
    final int unit_scale = unitScale;
    final float max_value = maxValue;
    final float min_value = minValue;

    Configuration eraIntrimConf = new Configuration();
    eraIntrimConf.set("dataType", "float");
    eraIntrimConf.setStrings("fileFormats", formats);
    eraIntrimConf.setStrings("variables", vars);
    eraIntrimConf.set("mapreduce.input.fileinputformat.inputdir", dataPath);
    eraIntrimConf.setStrings("corner", TaylorDiagramAnalysis.getCorner(lat_min, lat_max, lon_min, lon_max, shape));
    eraIntrimConf.setStrings("shape", TaylorDiagramAnalysis.getShape(lat_min, lat_max, lon_min, lon_max, shape));
    eraIntrimConf.setStrings("dimension", new String[]{"time", "lat", "lon"});
    eraIntrimConf.setInt("startDate", startTime);
    eraIntrimConf.setInt("endDate", endTime);

    JavaPairRDD<DataChunk, ArrayFloatSerializer> records = sc.newAPIHadoopRDD(eraIntrimConf, DataChunkInputFormat.class,
                                                                                       DataChunk.class,
                                                                                       ArrayFloatSerializer.class);
    JavaPairRDD<String, Tuple2<Float,Long>> sumCount = records.mapToPair(
        new PairFunction<Tuple2<DataChunk, ArrayFloatSerializer>, String, Tuple2<Float, Long>>() {
          @Override
          public Tuple2<String, Tuple2<Float, Long>> call(Tuple2<DataChunk, ArrayFloatSerializer> tuple2) throws Exception {
            ArrayFloat arrayFloat = tuple2._2().getArray();
            Float sum = 0.0f;
            Long count = 0L;
            for (int i=0; i<arrayFloat.getSize(); i++) {
              float v = arrayFloat.getFloat(i);
              if (v>min_value && v<max_value) {
                sum = sum + v;
                count++;
              }
            }
            return new Tuple2<String, Tuple2<Float, Long>>(tuple2._1().getFilePath()+"_"+tuple2._1().getVarShortName(), new Tuple2<Float, Long>(sum, count));
          }
        });

    JavaPairRDD<String, Float> meanRDD = sumCount.mapValues(
        new Function<Tuple2<Float, Long>, Float>() {
          @Override
          public Float call(Tuple2<Float, Long> v1) throws Exception {
            return v1._1()/v1._2()*unit_scale;
          }
        }).sortByKey(true);

    List<Float> eraIntrimMeanList = meanRDD.values().collect();
    Float[] eraIntrimMeanArray = new Float[eraIntrimMeanList.size()];
    eraIntrimMeanList.toArray(eraIntrimMeanArray);
    TaylorDiagramUnit eraIntrimUnit = new TaylorDiagramUnit(unitName, eraIntrimMeanArray);

    return eraIntrimUnit;
  }

  /**
   *
   * @param args
   * @throws ClassNotFoundException
   * @throws IOException
   * @throws InvalidRangeException
   * @throws InterruptedException
   */
  public static void main(final String[] args) throws ClassNotFoundException, IOException, InvalidRangeException, InterruptedException, ParseException {
    if (args.length != 22) {
      LOG.info("Please input 16 input paramters, start_time,end_time,min_lat,max_lat,min_lon,max_lon,spark_master,merra2Path, cfsr_path, cmap_path, gpcp_path, json_output, tydPythonScript,"
               + "x_axis,y_axis, tyd_output");
      return;
    }

    String start_time = args[0];  // "198001";
    String end_time = args[1];  //"198012";
    String min_lat = args[2];  //"-90.00";
    String max_lat = args[3];  //"90.00";
    String min_lon = args[4];  //"-180.00";
    String max_lon = args[5];  //"180.00";
    String spark_master = args[6];  //"local[6]";
    String merra2Path = args[7]; //"/Users/feihu/Documents/Data/Merra2/monthly";
    String cfsr_path = args[8];  //"/Users/feihu/Documents/Data/CFSR/pgbh06.gdas.A_PCP.SFC.grb2";
    String cmap_path = args[9];  //"/Users/feihu/Documents/Data/CMAP/precip.mon.mean.standard.nc";
    String gpcp_path = args[10]; //"/Users/feihu/Documents/Data/GPCPV2.2/precip.mon.mean.nc";
    String json_output = args[11];  //"/Users/feihu/Desktop/taylordiagram.json";
    String tydPythonScript = args[12];  //"/Users/feihu/Documents/GitHub/stc-geohpc/stc-datavisualization/src/main/java/edu/gmu/stc/datavisualization/python/TaylorDiagram.py"; //"/Users/feihu/Desktop/taylordiagram-v2.py";
    String x_axis = args[13];  //"Precipitation";
    String y_axis = args[14];  //"values(mm/day)";
    String tyd_output = args[15];  //"/Users/feihu/Desktop/taylordiagram-reanalysis.png";
    String isCMAP = args[16];
    String isGPCP = args[17];
    String isMERRA2 = args[18];
    String isMERRA1 = args[19];
    String isCFSR = args[20];
    String isERAINTRIM = args[21];

    int startTime = Integer.parseInt(start_time);
    int endTime = Integer.parseInt(end_time);
    float lat_min = Float.parseFloat(min_lat);
    float lat_max = Float.parseFloat(max_lat);
    float lon_min = Float.parseFloat(min_lon);
    float lon_max = Float.parseFloat(max_lon);

    int[] MERRA2_SHAPE = new int[]{1, 361, 576};
    int[] MERRA1_SHAPE = new int[]{1, 361, 540};
    int[] ERA_INTRIM_SHAPE = new int[]{1, 256, 512};

    final SparkConf sconf = new SparkConf().setAppName("SparkTest").setMaster(spark_master);
    sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sconf.set("spark.kryo.registrator", SparkKryoRegistrator.class.getName());
    sconf.set("spark.driver.allowMultipleContexts", "true");

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


    List<TaylorDiagramUnit> taylorDiagramUnits = new ArrayList<TaylorDiagramUnit>();

    TaylorDiagramUnit cmapUnit = TaylorDiagramAnalysis.cmapmean(fs, cmap_path, lat_min, lat_max, lon_min, lon_max, startTime, endTime);
    float[] yFloat = cmapUnit.getValues();
    double[] yArray = new double[yFloat.length];
    for (int i=0; i<yArray.length; i++) {
      yArray[i] = yFloat[i];
    }

    cmapUnit.calSTDAndPersonsCorrelation(yArray);
    taylorDiagramUnits.add(cmapUnit);

    if (Boolean.parseBoolean(isGPCP)) {
      TaylorDiagramUnit gpcpUnit = TaylorDiagramAnalysis.gpcpmean(fs, gpcp_path, lat_min, lat_max, lon_min, lon_max, startTime, endTime);
      gpcpUnit.calSTDAndPersonsCorrelation(yArray);
      taylorDiagramUnits.add(gpcpUnit);
    }

    if (Boolean.parseBoolean(isMERRA2)) {
      TaylorDiagramUnit merra2Unit = TaylorDiagramAnalysis.generateTaylorDiagramUnit(sc, "MERRA-2", merra2Path, new String[]{"nc4"}, new String[]{"PRECTOT"},
                                                                                     1000.0f, 0.0f,
                                                                                     lat_min, lat_max, lon_min, lon_max, MERRA2_SHAPE,
                                                                                     startTime, endTime, 3600*24);
      merra2Unit.calSTDAndPersonsCorrelation(yArray);
      taylorDiagramUnits.add(merra2Unit);
    }

    if (Boolean.parseBoolean(isCFSR)) {
      TaylorDiagramUnit cfsrUnit = TaylorDiagramAnalysis.cfsrmean(fs, cfsr_path, lat_min, lat_max, lon_min, lon_max, startTime, endTime);
      cfsrUnit.calSTDAndPersonsCorrelation(yArray);
      taylorDiagramUnits.add(cfsrUnit);
    }

    if (Boolean.parseBoolean(isERAINTRIM)) {
      TaylorDiagramUnit eraIntrimUnit = TaylorDiagramAnalysis.generateTaylorDiagramUnit(sc, "ERA-INTRIM",
                                                                                        "/Users/feihu/Documents/Data/era-interim/", new String[]{"00"},
                                                                                        new String[]{"Total_precipitation_surface_12_Hour_120"},
                                                                                        1000.0f, 0.0f,
                                                                                        lat_min, lat_max, lon_min, lon_max, ERA_INTRIM_SHAPE, startTime*10000+100, endTime*10000+100, 1000);
      eraIntrimUnit.calSTDAndPersonsCorrelation(yArray);
      taylorDiagramUnits.add(eraIntrimUnit);
    }

    if (Boolean.parseBoolean(isMERRA1)) {
      TaylorDiagramUnit merra1Unit = TaylorDiagramAnalysis.generateTaylorDiagramUnit(sc, "MERRA-1",
                                                                                     "/Users/feihu/Documents/Data/Merra/", new String[]{"hdf"},
                                                                                     new String[]{"PRECTOT"},
                                                                                     1000.0f, 0.0f,
                                                                                     lat_min, lat_max, lon_min, lon_max, MERRA1_SHAPE, startTime, endTime,3600*24);
      merra1Unit.calSTDAndPersonsCorrelation(yArray);
      taylorDiagramUnits.add(merra1Unit);
    }

    TaylorDiagramFactory.savetoJSON(taylorDiagramUnits, json_output);
    TaylorDiagramFactory.drawTaylorDiagram(tydPythonScript, json_output, x_axis, y_axis, tyd_output);

  }
}
