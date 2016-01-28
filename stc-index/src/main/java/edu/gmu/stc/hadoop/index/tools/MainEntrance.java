package edu.gmu.stc.hadoop.index.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import edu.gmu.stc.configure.MyProperty;
import edu.gmu.stc.database.IndexOperator;

/**
 * Created by feihu on 12/29/15.
 */
public class MainEntrance {

    public void initConfig(String path) {
        BufferedReader br;

        try {
            String configFile = new String(); //= System.getProperty( "os.name" ).contains( "indow" ) ? "E://config.txt" : "/Users/feihu/Documents/GitHub/oozie-master/workflowgenerator/src/main/webapp/config.txt";
            String osVersion = System.getProperty("os.name");

	    	/*if(osVersion.contains("indow")) {
	    		configFile = "E://config.txt";
	    	} else if(osVersion.contains("Mac")) {
	    		configFile = "/Users/feihu/Documents/GitHub/oozie-master/workflowgenerator/src/main/webapp/config.txt";
	    	} else {
	    		configFile = "/home/config.txt";
	    	}*/

            configFile = path;
            br = new BufferedReader(new FileReader(configFile));

            String line = br.readLine();

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

            br.close();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }


    public static void main(String args[]) {
        MainEntrance me = new MainEntrance();
        me.initConfig(args[0]);

        String[] params = new String[args.length-1];

        for(int i=0; i<params.length; i++) {
            params[i] = args[i+1];
        }

        IndexOperator.main(params);
        //String inputs[] = new String[] {"CreateTables", "/Users/feihu/Documents/Data/Merra/MERRA100.prod.simul.tavgM_2d_mld_Nx.198001.hdf"};
        //String inputs[] = new String[] {"AddRecords", "/Users/feihu/Documents/Data/Merra/", "198001", "201412"};
        //IndexOperator.main(inputs);


    }
}
