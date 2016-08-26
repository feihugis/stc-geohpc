package edu.gmu.stc.spark.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.configure.MyProperty;

/**
 * Created by Fei Hu on 8/25/16.
 */
public class STIndexUtils {

  public static List<String> getInputPaths(String inputDir, String fileFormat) {
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
        if (location.endsWith(fileFormat)) {
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

}
