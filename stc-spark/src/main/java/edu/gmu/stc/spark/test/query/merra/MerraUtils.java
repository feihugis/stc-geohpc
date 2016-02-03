package edu.gmu.stc.spark.test.query.merra;

import ucar.nc2.Group;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.IOException;
import java.util.List;

/**
 * Created by Fei Hu on 1/27/16.
 */
public class MerraUtils {
    public MerraUtils() {
    }

    static String getVarNames(String path) {
        String variables = "";
        try {
            NetcdfFile hdf = NetcdfFile.open(path);
            List<Group> groups = hdf.getRootGroup().getGroups();
            for (Group grp : groups) {
                Group data_fileds = grp.getGroups().get(0);
                List<Variable> variableList = data_fileds.getVariables();
                for (Variable var : variableList) {
                    variables = variables + "," + var.getShortName();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(variables);
        return variables;
    }

    public static void main(String[] args) {
        MerraUtils.getVarNames("/Users/feihu/Documents/Data/MERRA300.prod.assim.tavg1_2d_int_Nx.20140101.hdf");
    }
}
