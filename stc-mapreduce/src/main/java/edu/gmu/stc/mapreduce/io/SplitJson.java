package edu.gmu.stc.mapreduce.io;

import org.apache.hadoop.fs.Path;

/**
 * @author feihu
 * Save the information about the relation about filepath and host
 */
public class SplitJson {
	private Path _path = null;  // path of the in HDFS
	
	// Array of host names that this split *should* be assigned to 
	private String[] _hosts = null; 
	
	public SplitJson(Path path, String[] hosts) {
		this._path = new Path(path.toString());
		this._hosts = new String[hosts.length];
	    for (int i=0; i<this._hosts.length; i++) {
	      this._hosts[i] = hosts[i];
	    } 
	}
	
	public Path getPath(){
		return this._path;
	}
	
	public String[] getHosts() {
		return this._hosts;
	}
	
}
