package edu.gmu.stc;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import ucar.ma2.Array;
import ucar.nc2.Group;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {

        try {
            String file = "17";
            NetcdfFile hdf = NetcdfFile.open("/Users/feihu/Documents/Data/modis_hdf/" + file + ".hdf");
            List<Group> groups = hdf.getRootGroup().getGroups();


            for (Group grp : groups) {
                PrintWriter writer = new PrintWriter("/Users/feihu/Desktop/modis/"+ file + "_" + grp.getFullName() + ".txt", "UTF-8");
                Variable HDFEOS_CRS = grp.getVariables().get(0);
                double x_leftup, y_leftup, x_rightlow, y_rightlow, x_unit, y_unit;
                x_leftup = (double) HDFEOS_CRS.findAttribute("UpperLeftPointMtrs").getValue(0);
                y_leftup = (double) HDFEOS_CRS.findAttribute("UpperLeftPointMtrs").getValue(1);
                x_rightlow = (double) HDFEOS_CRS.findAttribute("LowerRightMtrs").getValue(0);
                y_rightlow = (double) HDFEOS_CRS.findAttribute("LowerRightMtrs").getValue(1);


                Group data_fileds = grp.getGroups().get(0);

                List<Variable> variableList = data_fileds.getVariables();

                String header = "x\ty\t";

                List<Array> valuesList = new ArrayList<Array>();

                for (Variable var : variableList) {
                    header = header + var.getFullName() + "\t";
                    valuesList.add(var.read());
                }

                //writer.println(header);

                int[] shape = variableList.get(0).getShape();
                x_unit = (x_rightlow - x_leftup)/shape[0];
                y_unit = (y_rightlow - y_leftup)/shape[1];

                double x_tmp, y_tmp, v_tmp;

                for(int i=0; i <shape[0]; i++) {
                    for (int j=0; j<shape[1]; j++) {
                        x_tmp = x_leftup + x_unit*i;
                        y_tmp = y_leftup + y_unit*j;
                        String row = new String();
                        row = row + x_tmp + "\t";
                        row = row + y_tmp + "\t";

                        int idx = 0;
                        for (Variable var : variableList) {
                            v_tmp = valuesList.get(idx).getFloat(i * shape[0] + j);  //var.read().getFloat(i * shape[0] + j);
                            row = row + v_tmp + "\t";
                        }
                        writer.println(row);
                    }

                    System.out.println("Finish rows : " + i);
                }

                writer.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
