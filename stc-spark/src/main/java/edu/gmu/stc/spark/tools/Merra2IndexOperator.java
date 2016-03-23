package edu.gmu.stc.spark.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.configure.MyProperty;
import edu.gmu.stc.hadoop.raster.index.merra2.Merr2IndexBuilder;
import edu.gmu.stc.spark.io.kryo.SparkKryoRegistrator;

/**
 * Created by Fei Hu on 3/22/16.
 */
public class Merra2IndexOperator {

  public void initializeConfiguration(String configFile) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(configFile));
    String line = null;
    try {
      line = br.readLine();
      while (line != null) {
        System.out.println(line );
        if(line.split("=")[0].trim().equals("mysql_connString")){
          MyProperty.mysql_connString =line.split("=")[1].trim();
          System.out.println("====="+MyProperty.mysql_connString);
        } else if(line.split("=")[0].trim().equals("mysql_user")) {
          MyProperty.mysql_user =line.split("=")[1].trim();
        } else if(line.split("=")[0].trim().equals("mysql_password")) {
          MyProperty.mysql_password =line.split("=")[1].trim();
        } else if(line.split("=")[0].trim().equals("mysql_catalog")) {
          MyProperty.mysql_catalog =line.split("=")[1].trim();
        } else if(line.split("=")[0].trim().equals("nameNode")) {
          MyProperty.nameNode =line.split("=")[1].trim();
        }

        line = br.readLine();
      }
    } catch (IOException e1) {
      e1.printStackTrace();
    }

    br.close();
  }

  public List<String> getInputPaths(String inputDir) {
    List<String> files = new ArrayList<String>();
    Path path = new Path(inputDir);

    try {
      Configuration conf = new Configuration();
      conf.set("fs.defaultFS", MyProperty.nameNode);
      conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
      conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
      conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
      FileSystem fs = FileSystem.get(conf);
      List<FileStatus> fileStatusList = new ArrayList<FileStatus>();
      RemoteIterator<LocatedFileStatus> ritr = fs.listFiles(path, true);
      while (ritr.hasNext()) {
        FileStatus file = ritr.next();
        String location = file.getPath().toString();
        String[] paths = location.split("\\.");
        String format = paths[paths.length - 1];
        if (format.equalsIgnoreCase("hdf")||format.equalsIgnoreCase("nc4")) {
          fileStatusList.add(file);
          files.add(file.getPath().toString());
        } else {
          continue;
        }
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return files;
  }

  public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException {
    String configFile = args[0];
    String inputDir = args[1]; //"/Users/feihu/Documents/Data/Merra2/";
    if (args.length != 2) {
      System.out.println("Please input <configFilePath> <inputDir>");
      return;
    }

    Merra2IndexOperator merra2IndexOperator = new Merra2IndexOperator();
    //merra2IndexOperator.initializeConfiguration(configFile);
    List<String> inputFiles = merra2IndexOperator.getInputPaths(inputDir);

    Merr2IndexBuilder merra2IndexBuilder = new Merr2IndexBuilder();

    merra2IndexBuilder.initMetaIndexTable();
    merra2IndexBuilder.insertMerra2SpaceIndex();
    //merra2IndexBuilder.closeDBConnnection();

    final SparkConf sconf = new SparkConf().setAppName("SparkTest");//.setMaster("local[6]");

    sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sconf.set("spark.kryo.registrator", SparkKryoRegistrator.class.getName());

    JavaSparkContext sc = new JavaSparkContext(sconf);


    JavaRDD<String> inputMerra2 = sc.parallelize(inputFiles);

    inputMerra2.foreach(new VoidFunction<String>() {
      @Override
      public void call(String s) throws Exception {
        List<String> files = new ArrayList<String>();
        files.add(s);
        Merr2IndexBuilder merra2IndexBuilder = new Merr2IndexBuilder();
        merra2IndexBuilder.createFileIndexTablesInBatch(files);
        merra2IndexBuilder.insertdataChunks(files);
        //merra2IndexBuilder.closeDBConnnection();
      }
    });
  }
}
