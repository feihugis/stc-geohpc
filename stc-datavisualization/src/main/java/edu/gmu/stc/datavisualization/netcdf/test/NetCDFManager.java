package edu.gmu.stc.datavisualization.netcdf.test;

import edu.gmu.stc.configure.MyProperty;
import edu.gmu.stc.datavisualization.netcdf.FloatCoordinate;
import edu.gmu.stc.datavisualization.netcdf.FloatCoordinateSystem;
import edu.gmu.stc.datavisualization.netcdf.GifSequenceWriter;
import edu.gmu.stc.datavisualization.netcdf.MutableImage;
import edu.gmu.stc.datavisualization.netcdf.PngColor;
import edu.gmu.stc.datavisualization.netcdf.PngWriter;
import edu.gmu.stc.datavisualization.netcdf.StopWatch;
import ucar.ma2.ArrayFloat;
import ucar.ma2.ArrayFloat.D2;
import ucar.ma2.ArrayFloat.D3;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.*;
import ucar.nc2.Dimension;


import javax.imageio.ImageIO;
import javax.imageio.stream.FileImageOutputStream;
import javax.imageio.stream.ImageOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.awt.*;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;
import java.util.Map.Entry;

import static ucar.ma2.ArrayFloat.D1;

@SuppressWarnings("deprecation")
public class NetCDFManager {

    private static final Font font = new Font("", Font.BOLD, 16);
    private static final SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd");
    Comparator<Float> compFloat = new Comparator<Float>() {
        public int compare(Float a, Float b) {
            return (a.compareTo(b));
        }
    };

    public static void main(String args[]) {
        NetCDFManager myN = new NetCDFManager();
        ArrayList<String> fileList = new ArrayList<String>();
        try {
            Scanner s = new Scanner(new File("input.txt"));
            while (s.hasNextLine()) {
                fileList.add(s.nextLine());
            }
            s.close();

            if (fileList.size() == 0)
                System.out.println("filelist.txt is empty");

        } catch (Exception e) {
            System.out.println("Could not read filelist.txt");
        }

        ArrayList<String> fileList2 = new ArrayList<String>();
        try {
            Scanner s = new Scanner(new File("input2.txt"));
            while (s.hasNextLine()) {
                fileList2.add(s.nextLine());
            }
            s.close();

            if (fileList2.size() == 0)
                System.out.println("filelist2.txt is empty");

        } catch (Exception e) {
            System.out.println("Could not read filelist2.txt");
        }

        StopWatch watch = new StopWatch();
        //myN.printNetCDFVarInfo(fileList.get(0), "area");
        //myN.printNetCDFInfo(fileList.get(0));
        //myN.printWeightedMeanOfMonth(fileList.get(0),"tgrnd", 2);
        //myN.printMeanOfMonth(fileList.get(0),"tgrnd", 2);
        //myN.printStandardDeviationOfMonth(fileList.get(0),"tgrnd", 2);


        //myN.cloneNetCDF(fileList.get(0), "lel.nc");
        //myN.averageVariable(fileList, "lel.nc", "tgrnd");
        //myN.averageVariableTxt(fileList, "lel.txt", "GRN");
        //myN.printNetCDFInfo("lel.nc");
        //myN.printNetCDFVarInfo("lel.nc", "tgrnd");
        //myN.printNetCDFInfo(fileList.get(0));
        //myN.printNetCDFVarInfo(fileList.get(0), "tgrnd");
        //myN.printNetCDFVarInfo(fileList.get(1), "tgrnd");
        //System.out.println(myN.findMax(fileList, "tgrnd"));
        //myN.makeLegend(fileList.get(0), "ground temp", "tgrnd", "january");
        // myN.makeChart(fileList2.get(0), "test.png", "tgrnd", 6, 4, 2f,true,true,new Date(51,0,1),-90,50);
        //myN.makeCharts(fileList2,"whuuut.png","tgrnd",4,2f,true, true,true,new Date(51,0,01));

        //myN.makeGif(fileList2, "haha.gif");

        //myN.interpolateData(fileList.get(0),"testerino.nc","tgrnd",4,2f);
        //myN.printNetCDFInfo("testerino.nc");
        //myN.makeChartLiteral("testerino.nc","thequest.png","tgrnd",0,4,true);
        //myN.makeChart(fileList2.get(0),"thequestpt1.png","tgrnd",0,8,2f,true);

        //myN.makeChartNearestNeighbor(fileList2.get(0),"pjnn","tgrnd",0,8,true,true,new Date(51,0,01));
        //myN.makeChart(fileList2.get(0),"pjidw","tgrnd",0,8,2f,true,true,new Date(51,0,01));
        //myN.printNetCDFInfo(fileList.get(0));
        //myN.makeCharts(fileList,"RZMC","RZMC",4,2f,true,false);

        //myN.makeChart(fileList.get(0), "TAST","SPSNOW",0,2,2f,true); //took 2472235ms
        //myN.makeChartBilinear(fileList2.get(0),"ncdf","tgrnd",0,8,true,true,new Date(51,0,01));
        //myN.makeGifOfVariable(fileList2.get(0), "tgrnd", "66bil", 8, 100, true, true, new Date(51, 0, 01));
        //myN.printNetCDFVarInfo(fileList.get(0),"GRN");
        //myN.makeChartNearestNeighbor(fileList2.get(0),"LARDALMIGHTY.png", "tgrnd", 6, 4,true,true,new Date(51,0,1));
        //myN.makeChartBilinearForEachVar(fileList.get(0),"RA",2,true,true,new Date(0,0,01));
        //myN.printAllVariableData(fileList.get(0));

        //myN.averageVariableTxt(fileList,"HDFyo","GRN");
        //myN.printNetCDFInfo(fileList.get(0));
        //myN.makeChartBilinear(fileList.get(0),"MISSINGNO","GRN",0,2,true,true,new Date(51,0,1));
        //myN.makeGifOfVariableBilinear(fileList.get(0), "MISSINGOFAA", "GRN", 2, 200, true, true, new Date(51, 0, 01));
        //myN.makeChartBilinear(fileList.get(0), "MISSINGOFBB", "GRN", 44, 2, true, true, new Date(51, 0, 01));
        //myN.makeGifOfVariableBilinear(fileList2.get(0), "tgrnd", "66bil", 8, 100, true, true, new Date(51, 0, 01));

        //myN.interpolateData(fileList.get(0),"TESTERINO.nc","GRN",2,2f);
        myN.interpolateDataHdf(fileList.get(0), "Tset.nc", "GRN", 2, 2f);
        myN.makeChartBilinear("Tset.nc", "hahareally", "GRN", 0, 2, true, true, new Date(51, 0, 1));
        //myN.makeChart(fileList.get(0),"#haes","GRN",0,2,2f,true,true, new Date(51,0,01));
        //myN.interpolateData(fileList2.get(0),"ADGASDG.nc","tgrnd",2,2f);
        //myN.makeChartBilinear("ADGASDG.nc","H111","tgrnd",0,2,true,true,new Date(51,0,01));

/*

        watch.reset();
        watch.start();
        myN.makeChart(fileList2.get(0),"qyuzIdw.png","tgrnd", 0 , 4 ,2f, true);
        watch.stop();
        System.out.println(watch.getElapsedTime());

        watch.reset();
        watch.start();
        myN.makeChartNearestNeighbor(fileList2.get(0), "qyuzNN.png", "tgrnd", 0, 4, true);
        watch.stop();
        System.out.println(watch.getElapsedTime());



        watch.reset();
        watch.start();
        myN.makeChartBilinear(fileList2.get(0), "qyuzBilinear.png", "tgrnd", 0, 4, true);
        watch.stop();
        System.out.println(watch.getElapsedTime());

        watch.reset();
        watch.start();
        myN.makeChartBicubic(fileList2.get(0), "qyuzBicubic.png", "tgrnd", 0, 4, true);
        watch.stop();
        System.out.println(watch.getElapsedTime());


        */
                /*

		for(int i=0;i<12;i++)
		{
			myN.printStandardDeviationOfMonth(myN.fileList.get(0),"tgrnd",i);
		}
		for(int i=0;i<12;i++)
		{
			myN.printWeightedMeanOfMonth(myN.fileList.get(0),"tgrnd",i);
		}

*/

        //parseDimensions(fileList.get(0));

        //StopWatch myWatch = new StopWatch();


        //myWatch.start();
        //writeNetCDFAverageAllVariables("test5.nc");
        //myWatch.stop();
        //	System.out.println("=======done!=======");

		/*
        try
		{
			NetcdfFile myn = NetcdfFile.open(fileList.get(0), null);
			System.out.println(myn.toString());

			Variable v = myn.findVariable("tgrnd");

		}
		catch(Exception e)
		{

		}*/

        //System.out.println(myn.toString());
        //System.out.println(myn.getGlobalAttributes());
        /*
        for(Attribute e : myn.getGlobalAttributes())
		{
			System.out.println(e.getFullName());
			System.out.println(e.getStringValue());
		}*/
        //writeNetCDFAverage("test2.nc",var);
        //addGeo2D(fileList.get(0));
        //readNetCDF("test.nc",var);
    }

    /**
     * printNetCDFInfo
     * opens a NetCDF file and prints the overview provided by the NetCDF toString() function
     *
     * @param fileName file path of the NetCDF file
     */
    public void printNetCDFInfo(String fileName) {
        NetcdfFile dataFile = null;
        try {
            dataFile = NetcdfFile.open(fileName, null);
            System.out.println(dataFile.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        } finally {
            if (dataFile != null)
                try {
                    dataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        }
    }

    public void printAllVariableData(String fileName) {
        NetcdfFile dataFile = null;
        try {
            dataFile = NetcdfFile.open(fileName, null);
            //System.out.println(dataFile.toString());
            // Retrieve the variable

            for (Variable dataVar : dataFile.getVariables()) {


                // Read dimension information on the variable
                int[] shape = dataVar.getShape();
                int[] origin = new int[3];

                if (shape.length > 2) {

                    D3 dataArray;
                    dataArray = (D3) dataVar.read(origin, shape);

                    //float[][][] dataArray = new float[shape[0]][shape[1]][shape[2]];

                    System.out.println("=========== Data for " + dataVar.getFullName() + " ===========");

                    for (int i = 0; i < shape[0]; i++)    //time
                    {
                        for (int j = 0; j < shape[1]; j++) {
                            System.out.print("[");
                            for (int k = 0; k < shape[2]; k++) {
                                if (k == shape[2] - 1)
                                    System.out.print("" + dataArray.get(i, j, k));
                                else
                                    System.out.print("" + dataArray.get(i, j, k) + ", \t");
                            }
                            System.out.println("]");
                        }
                        System.out.println("");
                    }
                    System.out.println("=========== Data for " + dataVar.getFullName() + " ===========");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        } catch (InvalidRangeException e) {

            e.printStackTrace();
        } finally {
            if (dataFile != null)
                try {
                    dataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        }
    }

    /**
     * printNetCDFVarInfo
     * Opens a NetCDF file (.hdf compatible), searches for a variable, and prints all the variable data
     *
     * @param fileName file path of the NetCDF file
     * @param var      variable to print data
     */
    public void printNetCDFVarInfo(String fileName, String var) {
        NetcdfFile dataFile = null;
        try {
            dataFile = NetcdfFile.open(fileName, null);
            //System.out.println(dataFile.toString());
            // Retrieve the variable
            Variable dataVar = dataFile.findVariable(var);

            if (dataVar == null)
                for (Variable v : dataFile.getVariables()) {
                    if (v.getShortName().equals(var))
                        dataVar = v;
                }

            if (dataVar == null) {
                System.out.println("Cant find Variable data for: " + var);
                return;
            }
            // Read dimension information on the variable
            int[] shape = dataVar.getShape();
            int[] origin = new int[3];

            D3 dataArray;
            dataArray = (D3) dataVar.read(origin, shape);

            //float[][][] dataArray = new float[shape[0]][shape[1]][shape[2]];

            System.out.println("=========== Data for " + dataVar.getFullName() + " ===========");

            for (int i = 0; i < shape[0]; i++)    //time
            {
                for (int j = 0; j < shape[1]; j++) {
                    System.out.print("[");
                    for (int k = 0; k < shape[2]; k++) {
                        if (k == shape[2] - 1)
                            System.out.print("" + dataArray.get(i, j, k));
                        else
                            System.out.print("" + dataArray.get(i, j, k) + ", \t");
                    }
                    System.out.println("]");
                }
                System.out.println("");
            }
            System.out.println("=========== Data for " + dataVar.getFullName() + " ===========");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        } catch (InvalidRangeException e) {

            e.printStackTrace();
        } finally {
            if (dataFile != null)
                try {
                    dataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        }
    }

    /**
     * getMean
     * Calculates the mean value of a 3D float array
     *
     * @param ar ArrayFloat.D3 array of values
     * @return the mean as a float
     */
    public float getMean(D3 ar) {
        float ret = 0f;
        int[] shape = ar.getShape();
        float total = shape[0] * shape[1] * shape[2];

        for (int i = 0; i < shape[0]; i++) {
            for (int j = 0; j < shape[1]; j++) {
                for (int k = 0; k < shape[2]; k++) {
                    ret += ar.get(i, j, k);
                }
            }
        }
        return ret / total;
    }

    /**
     * getMean
     * Calculates the mean value of a 2D float array
     *
     * @param ar ArrayFloat.D2 array of values
     * @return the mean as a float
     */
    public float getMean(D2 ar) {
        float ret = 0f;
        int[] shape = ar.getShape();
        float total = shape[0] * shape[1];

        for (int i = 0; i < shape[0]; i++) {
            for (int j = 0; j < shape[1]; j++) {
                ret += ar.get(i, j);
            }
        }
        return ret / total;
    }

    /**
     * getMean
     * Calculates the mean value of a 1D float array
     *
     * @param ar ArrayFloat.D1 array of values
     * @return the mean as a float
     */
    public float getMean(D1 ar) {
        float ret = 0f;
        int[] shape = ar.getShape();
        float total = shape[0] * shape[1];

        for (int i = 0; i < shape[0]; i++) {
            ret += ar.get(i);
        }
        return ret / total;
    }

    /**
     * getAreaWeightedMean
     * calculates the mean value of a 2D float array with area weights along the latitude axis
     *
     * @param data  ArrayFloat.D2 array of values
     * @param areas ArrayFloat.D1 array of weights
     * @return the weighted mean as a float
     */
    public float getAreaWeightedMean(D2 data, D1 areas) {
        float ret = 0f;
        float areaSum = 0f;
        int[] dataShape = data.getShape();

        for (int i = 0; i < dataShape[0]; i++)            //latitude
        {
            for (int j = 0; j < dataShape[1]; j++)    //longitude
            {
                ret += data.get(i, j) * areas.get(i); //sum of DATAij * AREAi; data and corresponding area weight of latitude amount
                areaSum += areas.get(i);
            }
        }
        return ret / areaSum;
    }

    /**
     * getStandardDeviation
     * returns the standard deviation of a 2D float array
     *
     * @param ar 2D array of values
     * @return mean as a float
     */
    public float getStandardDeviation(D2 ar) {
        float ret = 0;
        float mean = getMean(ar);
        int[] shape = ar.getShape();
        for (int i = 0; i < shape[0]; i++) {
            for (int j = 0; j < shape[1]; j++) {
                ret += (ar.get(i, j) - mean) * (ar.get(i, j) - mean);
            }
        }
        ret = ret / ((float) shape[0] * shape[1]);
        ret = (float) Math.sqrt(ret);
        return ret;
    }

    /**
     * getAreaWeightedStandardDeviation
     * returns the standard deviation of a 2D float array based on a 1D float array of area weights
     *
     * @param ar    2D array of values
     * @param areas 1D array of weights
     * @return the area weighted mean as a float
     */
    public float getAreaWeightedStandardDeviation(D2 ar, D1 areas) {
        float ret = 0;
        float mean = getAreaWeightedMean(ar, areas);
        int[] shape = ar.getShape();
        for (int i = 0; i < shape[0]; i++) {
            for (int j = 0; j < shape[1]; j++) {
                ret += (ar.get(i, j) - mean) * (ar.get(i, j) - mean);
            }
        }
        ret = ret / ((float) shape[0] * shape[1]);
        ret = (float) Math.sqrt(ret);
        return ret;
    }

    /**
     * generateArrayD1
     * helper function for writing to netCDF files and preserving the data structure format of variables
     * gives a 1D array of the values of a variable
     *
     * @param fileName netCDF file to open
     * @param var      variable name
     * @return ArrayFloat.D1 of the values of the variable
     */
    public D1 generateArrayD1(String fileName, String var) {
        NetcdfFile dataFile = null;
        D1 ret = null;
        try {
            int[] origin = new int[1];
            dataFile = NetcdfFile.open(fileName);
            Variable dataVar = dataFile.findVariable(var);
            if (dataVar == null) {
                System.out.println("Cant find Variable data " + var);
                return null;
            }
            ret = (D1) dataVar.read(origin, dataVar.getShape());
        } catch (Exception e) {
            if (dataFile != null)
                try {
                    dataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        }
        return ret;
    }

    /**
     * generateArrayD2
     * helper function for writing to netCDF files and preserving the data structure format of variables
     * gives a 2D array of the values of a variable
     *
     * @param fileName netCDF file to open
     * @param var      variable name
     * @return ArrayFloat.D2 of the values of the variable
     */
    public D2 generateArrayD2(String fileName, String var) {
        NetcdfFile dataFile = null;
        D2 ret = null;
        try {
            int[] origin = new int[2];
            dataFile = NetcdfFile.open(fileName);
            Variable dataVar = dataFile.findVariable(var);
            //System.out.println(dataVar.read(origin,dataVar.getShape()).toString());
            if (dataVar == null) {
                System.out.println("Cant find Variable data " + var);
                return null;
            }
            ret = (D2) dataVar.read(origin, dataVar.getShape());
        } catch (Exception e) {
            if (dataFile != null)
                try {
                    dataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        }
        return ret;
    }


    /**
     * printMeanOfMonth
     * Prints out the mean of a variable in a specified month
     *
     * @param fileName  file path of the NetCDF file
     * @param var       variable to print
     * @param monthName which month to analyze
     */
    public void printMeanOfMonth(String fileName, String var, String monthName) {
        NetcdfFile dataFile = null;
        int month = convertMonthToInt(monthName);
        try {
            if (month < 0 || month > 11)                        //check if the month is valid
            {
                System.out.println("Invalid Month (" + monthName + "). Could not print mean of (" + var + ").");
                return;
            }
            dataFile = NetcdfFile.open(fileName);
            Variable dataVar = dataFile.findVariable(var);    //search the NetCDF file for the variable
            if (dataVar == null) {
                System.out.println("Variable not found. Could not print mean of (" + var + ").");
                return;
            }

            int[] origin = new int[dataVar.getShape().length];
            D3 dataArray = (D3) dataVar.read(origin, dataVar.getShape());                //load the 3D array of the data (longxlatxtime)
            D2 varData = new D2(dataVar.getShape(1), dataVar.getShape(2));    //create a new array to hold the data for the specified month
            for (int i = 0; i < dataVar.getShape(1); i++) {
                for (int j = 0; j < dataVar.getShape(2); j++) {
                    varData.set(i, j, dataArray.get(month, i, j));
                }
            }
            //System.out.println(varData);

            try {
                String units = dataVar.findAttribute("units").getStringValue();
                System.out.println("Mean of (" + var + ") in " + monthName + ": " + getMean(varData) + " " + units);
            } catch (Exception e) {
                System.out.println("Mean of (" + var + ") in " + monthName + ": " + getMean(varData));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != dataFile)
                try {
                    dataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        }

    }

    /**
     * printWeightedMeanOfMonth
     * Prints the weighted mean of a variable for a specific month
     *
     * @param fileName  file path of NetCDF file
     * @param var       variable to print
     * @param monthName which month to print
     */
    public void printWeightedMeanOfMonth(String fileName, String var, String monthName) {
        NetcdfFile dataFile = null;
        int month = convertMonthToInt(monthName);
        try {
            if (month < 0 || month > 11)        //check if the month is valid
            {
                System.out.println("Invalid Month (" + monthName + "). Could not print mean of (" + var + ").");
                return;
            }
            dataFile = NetcdfFile.open(fileName);
            Variable dataVar = dataFile.findVariable(var);    //search the NetCDF file for the variable and the area data
            if (dataVar == null) {
                System.out.println("Variable not found. Could not print mean of (" + var + ").");
                return;
            }
            Variable areaVar = dataFile.findVariable("area");
            if (areaVar == null) {
                System.out.println("Could not find variable (area), the weighted mean could not be computed.");
                return;
            }
            int[] origin = new int[areaVar.getShape().length];
            D2 areaDataSource = (D2) areaVar.read(origin, areaVar.getShape());    //load the area array source data
            D1 areaData = new D1(areaVar.getShape(1));                //latitude weights
            for (int i = 0; i < areaVar.getShape(1); i++) {
                areaData.set(i, areaDataSource.get(0, i));                    //write the data into a 1D array for latitude weights
            }

            origin = new int[dataVar.getShape().length];
            D3 dataArray = (D3) dataVar.read(origin, dataVar.getShape());                //load the variable data
            D2 varData = new D2(dataVar.getShape(1), dataVar.getShape(2));
            for (int i = 0; i < dataVar.getShape(1); i++) {
                for (int j = 0; j < dataVar.getShape(2); j++) {
                    varData.set(i, j, dataArray.get(month, i, j));                //isolate the variable data in a 2D array over a certain month
                }
            }
            //System.out.println(varData);
            try {
                String units = dataVar.findAttribute("units").getStringValue();
                System.out.println("Area Weighted Mean of (" + var + ") in " + monthName + ": " + getAreaWeightedMean(varData, areaData) + " " + units);
            } catch (Exception e) {
                System.out.println("Area Weighted Mean of (" + var + ") in " + monthName + ": " + getAreaWeightedMean(varData, areaData) + " (units not found)");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != dataFile)
                try {
                    dataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        }

    }

    /**
     * printStandardDeviationOfMonth
     * Prints out the standard deviation of a variable in a specified month
     *
     * @param fileName  file path of the NetCDF file
     * @param var       variable to print
     * @param monthName which month to analyze
     */
    public void printStandardDeviationOfMonth(String fileName, String var, String monthName) {
        NetcdfFile dataFile = null;
        int month = convertMonthToInt(monthName);
        try {
            if (month < 0 || month > 11)                        //check if the month is valid
            {
                System.out.println("Invalid Month (" + monthName + "). Could not print standard deviation of (" + var + ").");
                return;
            }
            dataFile = NetcdfFile.open(fileName);
            Variable dataVar = dataFile.findVariable(var);    //search the NetCDF file for the variable
            if (dataVar == null) {
                System.out.println("Variable not found. Could not print standard deviation of (" + var + ").");
                return;
            }
            Variable areaVar = dataFile.findVariable("area");    //search the NetCDF file for the area data
            if (areaVar == null) {
                System.out.println("Could not find variable (area), the weighted mean could not be computed.");
                return;
            }
            int[] origin = new int[areaVar.getShape().length];
            D2 areaDataArray = (D2) areaVar.read(origin, areaVar.getShape());    //load the latitude data source
            D1 areaData = new D1(areaVar.getShape(1));                //latitude weights
            for (int i = 0; i < areaVar.getShape(1); i++) {
                areaData.set(i, areaDataArray.get(0, i));                        //write the area weights into a 1D array
            }

            origin = new int[dataVar.getShape().length];
            D3 dataArray = (D3) dataVar.read(origin, dataVar.getShape());            //load the 3D data of the variable
            D2 varData = new D2(dataVar.getShape(1), dataVar.getShape(2));
            for (int i = 0; i < dataVar.getShape(1); i++) {
                for (int j = 0; j < dataVar.getShape(2); j++) {
                    varData.set(i, j, dataArray.get(month, i, j));        //isolate the variable data into a 2D array for a specific month
                }
            }
            try {
                String units = dataVar.findAttribute("units").getStringValue();
                System.out.println("Standard Deviation of (" + var + ") in " + monthName + ": " + getAreaWeightedStandardDeviation(varData, areaData) + " " + units);
            } catch (Exception e) {
                System.out.println("Standard Deviation of (" + var + ") in " + monthName + ": " + getAreaWeightedStandardDeviation(varData, areaData));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != dataFile)
                try {
                    dataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        }
    }

    /**
     * convertIntToMonth
     * given an int value for a month (0-11), return the name of the Month as a String
     *
     * @param month (0-11)
     * @return String of the month name
     */
    public String convertIntToMonth(int month) {
        String monthName;
        switch (month) {
            case 0:
                monthName = "January";
                break;
            case 1:
                monthName = "February";
                break;
            case 2:
                monthName = "March";
                break;
            case 3:
                monthName = "April";
                break;
            case 4:
                monthName = "May";
                break;
            case 5:
                monthName = "June";
                break;
            case 6:
                monthName = "July";
                break;
            case 7:
                monthName = "August";
                break;
            case 8:
                monthName = "September";
                break;
            case 9:
                monthName = "October";
                break;
            case 10:
                monthName = "November";
                break;
            case 11:
                monthName = "December";
                break;
            default:
                monthName = "Invalid Month";
                break;
        }
        return monthName;
    }

    /**
     * convertMonthToInt
     * given the name of the Month as a String, return an int value (0-11)
     *
     * @param monthName month name as string
     * @return int value of the month, -1 if not found
     */
    public int convertMonthToInt(String monthName) {
        int month = -1;
        monthName = monthName.toLowerCase();
        if (monthName.equals("january"))
            month = 0;
        else if (monthName.equals("february"))
            month = 1;
        else if (monthName.equals("march"))
            month = 2;
        else if (monthName.equals("april"))
            month = 3;
        else if (monthName.equals("may"))
            month = 4;
        else if (monthName.equals("june"))
            month = 5;
        else if (monthName.equals("july"))
            month = 6;
        else if (monthName.equals("august"))
            month = 7;
        else if (monthName.equals("september"))
            month = 8;
        else if (monthName.equals("october"))
            month = 9;
        else if (monthName.equals("november"))
            month = 10;
        else if (monthName.equals("december"))
            month = 11;

        return month;
    }

    /**
     * expandArray
     * returns a 2D array scaled to a larger resolution, scaling up each data point to be a box of identical cells of scale width and height
     *
     * @param ar        2D array of values
     * @param scale     array resolution is lat*scale x long*scale
     * @return          2D array scaled up
     */
    public D2 expandArray(D2 ar, int scale) {
        int[] baseShape = ar.getShape();
        int[] shape = new int[baseShape.length];
        for (int i = 0; i < shape.length; i++) {
            shape[i] = baseShape[i] * scale;
        }
        D2 ret = new D2(shape[0], shape[1]);
        for (int i = 0; i < shape[0]; i++) {
            for (int j = 0; j < shape[1]; j++) {
                ret.set(i, j, ar.get(i / scale, j / scale));
            }
        }
        return ret;
    }

    /**
     * interpolateArray
     * This is a method of IDW interpolation to generate a larger 3D array of interpolated data based on a given 3D array for the latitude and longitude dimensions.
     * The given data is scaled up with slight variation, then each new data point is calculated using the IDW formula on
     * given data points that are within a range of scale * thresholdFactor.
     *
     * @param ar              original data
     * @param scale           new array dimensions of scale*given data dimension
     * @param thresholdFactor range that the IDW interpolation use (recommended: 2f)
     * @return a new ArrayFloat.D3 Array with interpolated data
     */
    public D3 interpolateArray(D3 ar, int scale, float thresholdFactor) {
        int[] baseShape = ar.getShape();
        FloatCoordinateSystem myFCS = new FloatCoordinateSystem(baseShape[1], baseShape[2]);    //create a new float coordinate system to import the data, essentially an ArrayList of 2D points
        D3 ret = new D3(baseShape[0], baseShape[1] * scale, baseShape[2] * scale);        //create the return array
        int[] shape = ret.getShape();
        float numer;
        float denom;
        float dist;
        float tempDenom;


        for (int t = 0; t < baseShape[0]; t++) {
            myFCS.clear();
            //load the data into an FloatCoordinateSystem ArrayList
            for (int i = 0; i < baseShape[1]; i++)
                for (int j = 0; j < baseShape[2]; j++)
                    myFCS.addCoor(i, j, ar.get(t, i, j));

            myFCS.scaleRandomCenter(scale);                        //scales the data up to a larger resolution with some variation of location
            TreeMap<Float, FloatCoordinate> myTree;                //go through each new cell of the new array and generate an interpolated point


            Entry<Float, FloatCoordinate> myE;

            //iterate through each point in the return array
            for (int i = 0; i < shape[1]; i++)        //row
            {
                for (int j = 0; j < shape[2]; j++)    //column
                {
                    myTree = new TreeMap<Float, FloatCoordinate>(compFloat);    //tree map to store the coordinates that are within range of the current data point
                    for (FloatCoordinate fc : myFCS.getArray()) {
                        dist = getDistance(i, j, fc.getX(), fc.getY());
                        if (dist < scale * thresholdFactor)                        //if the original data point is within a cell radius, then add it to the tree map
                            myTree.put(dist, fc);
                    }
                    numer = 0;
                    denom = 0;
                    myE = myTree.pollFirstEntry();                                // go through each data point in the tree map and apply the IDW formula
                    while (myE != null) {
                        dist = myE.getKey();
                        if (dist < scale / thresholdFactor / 2f)                        //special case if the current cell corresponds to an old data cell's location
                        {
                            //dist*=scale/(thresholdFactor*thresholdFactor);
                            dist = scale / thresholdFactor / 2f;                        //THIS REDUCES THE INTENSITY OF COLORS THAT OCCUR ON ORIGINAL DATA POINT LOCATIONS, SMOOTHING OUT COLORS
                        }

                        tempDenom = (1.0f / dist);
                        numer += myE.getValue().getValue() * tempDenom;                //applying the IDW formula
                        denom += tempDenom;
                        myE = myTree.pollFirstEntry();
                    }
                    ret.set(t, i, j, numer / denom);                                    //store the interpolated data point into the new array
                }
            }
        }
        return ret;
    }

    /**
     * interpolateArray
     * This is a method of IDW interpolation to generate a larger 2D array of interpolated data based on a given 2D array.
     * The given data is scaled up with slight variation, then each new data point is calculated using the IDW formula on
     * given data points that are within a range of scale * thresholdFactor.
     *
     * @param ar              original data
     * @param scale           new array dimensions of scale*given data dimension
     * @param thresholdFactor range that the IDW interpolation use (recommended: 2f)
     * @return a new ArrayFloat.D2 Array with interpolated data
     */
    public D2 interpolateArray(D2 ar, int scale, float thresholdFactor) {
        int[] baseShape = ar.getShape();
        FloatCoordinateSystem myFCS = new FloatCoordinateSystem(baseShape[0], baseShape[1]);    //create a new float coordinate system to import the data, essentially an ArrayList of 2D points
        D2 ret = new D2(baseShape[0] * scale, baseShape[1] * scale);        //create the return array

        //load the data into an FloatCoordinateSystem ArrayList
        for (int i = 0; i < baseShape[0]; i++)
            for (int j = 0; j < baseShape[1]; j++)
                myFCS.addCoor(i, j, ar.get(i, j));

        myFCS.scaleRandomCenter(scale);                        //scales the data up to a larger resolution with some variation of location
        TreeMap<Float, FloatCoordinate> myTree;                //go through each new cell of the new array and generate an interpolated point
        int[] shape = ret.getShape();


        float numer;
        float denom;
        float dist;
        float tempDenom;
        Entry<Float, FloatCoordinate> myE;

        //iterate through each point in the return array
        for (int i = 0; i < shape[0]; i++)        //row
        {
            for (int j = 0; j < shape[1]; j++)    //column
            {
                myTree = new TreeMap<Float, FloatCoordinate>(compFloat);    //tree map to store the coordinates that are within range of the current data point
                for (FloatCoordinate fc : myFCS.getArray()) {
                    dist = getDistance(i, j, fc.getX(), fc.getY());
                    if (dist < scale * thresholdFactor)                        //if the original data point is within a cell radius, then add it to the tree map
                        myTree.put(dist, fc);
                }
                numer = 0;
                denom = 0;
                myE = myTree.pollFirstEntry();                                // go through each data point in the tree map and apply the IDW formula
                while (myE != null) {
                    dist = myE.getKey();
                    if (dist < scale / thresholdFactor / 2f)                        //special case if the current cell corresponds to an old data cell's location
                    {
                        //dist*=scale/(thresholdFactor*thresholdFactor);
                        dist = scale / thresholdFactor / 2f;                        //THIS REDUCES THE INTENSITY OF COLORS THAT OCCUR ON ORIGINAL DATA POINT LOCATIONS, SMOOTHING OUT COLORS
                    }

                    tempDenom = (1.0f / dist);
                    numer += myE.getValue().getValue() * tempDenom;                //applying the IDW formula
                    denom += tempDenom;
                    myE = myTree.pollFirstEntry();
                }
                //System.out.println("("+i+", "+j+": "+numer/denom);
                ret.set(i, j, numer / denom);                                    //store the interpolated data point into the new array
            }
        }
        return ret;
    }

    /**
     * interpolateArray
     * This is a method of IDW interpolation to generate a larger 3D array of interpolated data based on a given 3D array for the latitude and longitude dimensions.
     * The given data is scaled up with slight variation, then each new data point is calculated using the IDW formula on
     * given data points that are within a range of scale * thresholdFactor.
     *
     * @param ar              original data
     * @param scale           new array dimensions of scale*given data dimension
     * @param thresholdFactor range that the IDW interpolation use (recommended: 2f)
     * @param ignoreValue     ignores a missing value in calculations and places the value in the interpolated array
     * @return a new ArrayFloat.D3 Array with interpolated data
     */
    public D3 interpolateArray(D3 ar, int scale, float thresholdFactor, float ignoreValue) {
        int[] baseShape = ar.getShape();
        FloatCoordinateSystem myFCS = new FloatCoordinateSystem(baseShape[1], baseShape[2]);    //create a new float coordinate system to import the data, essentially an ArrayList of 2D points
        D3 ret = new D3(baseShape[0], baseShape[1] * scale, baseShape[2] * scale);        //create the return array
        int[] shape = ret.getShape();
        float numer;
        float denom;
        float dist;
        float tempDenom;

        for (int t = 0; t < baseShape[0]; t++) {
            myFCS.clear();
            //load the data into an FloatCoordinateSystem ArrayList
            float val;
            for (int i = 0; i < baseShape[1]; i++)
                for (int j = 0; j < baseShape[2]; j++) {
                    val = ar.get(t, i, j);
                    if (val != ignoreValue)
                        myFCS.addCoor(i, j, val);
                }

            myFCS.scaleRandomCenter(scale);                        //scales the data up to a larger resolution with some variation of location
            TreeMap<Float, FloatCoordinate> myTree;                //go through each new cell of the new array and generate an interpolated point


            Entry<Float, FloatCoordinate> myE;

            //iterate through each point in the return array
            for (int i = 0; i < shape[1]; i++)        //row
            {
                for (int j = 0; j < shape[2]; j++)    //column
                {
                    val = ar.get(t, i / scale, j / scale);
                    if (val != ignoreValue)  //continue normally
                    {
                        myTree = new TreeMap<Float, FloatCoordinate>(compFloat);    //tree map to store the coordinates that are within range of the current data point
                        for (FloatCoordinate fc : myFCS.getArray()) {
                            dist = getDistance(i, j, fc.getX(), fc.getY());
                            if (dist < scale * thresholdFactor)                        //if the original data point is within a cell radius, then add it to the tree map
                                myTree.put(dist, fc);
                        }
                        numer = 0;
                        denom = 0;
                        myE = myTree.pollFirstEntry();                                // go through each data point in the tree map and apply the IDW formula
                        while (myE != null) {
                            dist = myE.getKey();
                            if (dist < scale / thresholdFactor / 2f)                        //special case if the current cell corresponds to an old data cell's location
                            {
                                //dist*=scale/(thresholdFactor*thresholdFactor);
                                dist = scale / thresholdFactor / 2f;                        //THIS REDUCES THE INTENSITY OF COLORS THAT OCCUR ON ORIGINAL DATA POINT LOCATIONS, SMOOTHING OUT COLORS
                            }

                            tempDenom = (1.0f / dist);
                            numer += myE.getValue().getValue() * tempDenom;                //applying the IDW formula
                            denom += tempDenom;
                            myE = myTree.pollFirstEntry();
                        }
                        ret.set(t, i, j, numer / denom);                                    //store the interpolated data point into the new array
                    } else {    //store the missing value again do not apply interpolation
                        ret.set(t, i, j, val);                                    //store the interpolated data point into the new array
                    }
                }
            }
        }
        return ret;
    }

    /**
     * interpolateArray
     * This is a method of IDW interpolation to generate a larger 2D array of interpolated data based on a given 2D array.
     * The given data is scaled up with slight variation, then each new data point is calculated using the IDW formula on
     * given data points that are within a range of scale * thresholdFactor.
     *
     * @param ar              original data
     * @param scale           new array dimensions of scale*given data dimension
     * @param thresholdFactor range that the IDW interpolation use (recommended: 2f)
     * @param ignoreValue     ignores a missing value in calculations and places the value in the interpolated array
     * @return a new ArrayFloat.D2 Array with interpolated data
     */
    public D2 interpolateArray(D2 ar, int scale, float thresholdFactor, float ignoreValue) {
        int[] baseShape = ar.getShape();
        FloatCoordinateSystem myFCS = new FloatCoordinateSystem(baseShape[0], baseShape[1]);    //create a new float coordinate system to import the data, essentially an ArrayList of 2D points
        D2 ret = new D2(baseShape[0] * scale, baseShape[1] * scale);        //create the return array

        //load the data into an FloatCoordinateSystem ArrayList
        float val;
        for (int i = 0; i < baseShape[0]; i++)
            for (int j = 0; j < baseShape[1]; j++) {
                val = ar.get(i, j);
                if (val != ignoreValue)
                    myFCS.addCoor(i, j, val);
            }

        myFCS.scaleRandomCenter(scale);                        //scales the data up to a larger resolution with some variation of location
        TreeMap<Float, FloatCoordinate> myTree;                //go through each new cell of the new array and generate an interpolated point
        int[] shape = ret.getShape();


        float numer;
        float denom;
        float dist;
        float tempDenom;
        Entry<Float, FloatCoordinate> myE;

        //iterate through each point in the return array

        for (int i = 0; i < shape[0]; i++)        //row
        {
            for (int j = 0; j < shape[1]; j++)    //column
            {
                val = ar.get(i / scale, j / scale);
                if (val != ignoreValue) {    //continue normally
                    myTree = new TreeMap<Float, FloatCoordinate>(compFloat);    //tree map to store the coordinates that are within range of the current data point
                    for (FloatCoordinate fc : myFCS.getArray()) {
                        dist = getDistance(i, j, fc.getX(), fc.getY());
                        if (dist < scale * thresholdFactor)                        //if the original data point is within a cell radius, then add it to the tree map
                            myTree.put(dist, fc);
                    }
                    numer = 0;
                    denom = 0;
                    myE = myTree.pollFirstEntry();                                // go through each data point in the tree map and apply the IDW formula
                    while (myE != null) {
                        dist = myE.getKey();
                        if (dist < scale / thresholdFactor / 2f)                        //special case if the current cell corresponds to an old data cell's location
                        {
                            //dist*=scale/(thresholdFactor*thresholdFactor);
                            dist = scale / thresholdFactor / 2f;                        //THIS REDUCES THE INTENSITY OF COLORS THAT OCCUR ON ORIGINAL DATA POINT LOCATIONS, SMOOTHING OUT COLORS
                        }

                        tempDenom = (1.0f / dist);
                        numer += myE.getValue().getValue() * tempDenom;                //applying the IDW formula
                        denom += tempDenom;
                        myE = myTree.pollFirstEntry();
                    }
                    //System.out.println("("+i+", "+j+": "+numer/denom);
                    ret.set(i, j, numer / denom);                                    //store the interpolated data point into the new array
                } else {
                    ret.set(i, j, ignoreValue); //assign the cell as a missing value
                }
            }
        }
        return ret;
    }


    /**
     * getDistance
     * returns a float calculation of the distance between two points using the distance formula
     *
     * @param x1
     * @param y1
     * @param x2
     * @param y2
     * @return distance
     */
    public float getDistance(int x1, int y1, int x2, int y2) {
        return (float) Math.sqrt(((float) (x2 - x1)) * ((float) (x2 - x1)) + ((float) (y2 - y1)) * ((float) (y2 - y1)));
    }

    /**
     * getDistance
     * returns a float calculation of the the distance between two points
     *
     * @param x1
     * @param y1
     * @param x2
     * @param y2
     * @return
     */
    public float getDistance(float x1, float y1, float x2, float y2) {
        return (float) Math.sqrt((x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1));
    }

    /**
     * getDistance
     * returns a float calculation of the the distance between two points
     *
     * @param x1
     * @param y1
     * @param floatCoordinate
     * @return
     */
    public float getDistance(int x1, int y1, FloatCoordinate floatCoordinate) {
        return (float) Math.sqrt((floatCoordinate.getX() - (x1)) * (floatCoordinate.getX() - x1) + (floatCoordinate.getY() - y1) * (floatCoordinate.getY() - y1));
    }


    /**
     * averageVariable
     * This function averages a given variable over a list of NetCDF files. A new NetCDF file is created that preserves the global attributes and dimensions of the given data, but only
     * has the variable being averaged. The variable in the new NetCDF file has the average in the origin of its data array and retains the same variable attributes and dimensions.
     * The average of the variable will also be written as a global attribute in the new NetCDF file.
     *
     * @param fileList   List of the files to be analyzed
     * @param outputName Name of the output NetCDF file
     * @param var        variable short name
     */
    public void averageVariable(ArrayList<String> fileList, String outputName, String var) {

        if (outputName.toLowerCase().contains(".nc"))
            outputName = outputName.substring(0, outputName.length() - 3);

        NetcdfFileWriteable outFile = new NetcdfFileWriteable();    //Create the file
        NetcdfFile myDataFile = null;                                //reference file

        try {
            outFile = NetcdfFileWriteable.createNew(outputName+".nc", false);
            myDataFile = NetcdfFile.open(fileList.get(0), null);


            Variable dataVar = myDataFile.findVariable(var);                //check if the variable is found

            if (dataVar == null)
                for (Variable v : myDataFile.getVariables()) {
                    if (v.getShortName().equals(var))
                        dataVar = v;
                }

            if (dataVar == null) {
                System.out.println("Cannot find Variable data " + var);
                return;
            }

            //Define DIMENSIONS
            List<Dimension> myListD = myDataFile.getDimensions();    //Global Dimensions of the new file

            for (Dimension d : myListD)                                //add the dimensions to the new file
            {
                outFile.addDimension(d.getFullName(), d.getLength(), d.isShared(), d.isUnlimited(), d.isVariableLength());
            }

            //Define GLOBAL ATTRIBUTES
            List<Attribute> AL = myDataFile.getGlobalAttributes();    //first, model all the global attributes off the first file in the filelist. Then, append the values for each attribute, separated by "; "
            List<Attribute> tempAL;
            for (int i = 1; i < fileList.size(); i++)                //skip over the first file since it is the base for the list
            {
                myDataFile = NetcdfFile.open(fileList.get(i), null);
                tempAL = myDataFile.getGlobalAttributes();
                for (int j = 0; j < AL.size(); j++)                    //iterate through each attribute for the AL and tempAL
                {
                    Attribute a = AL.get(j);
                    Attribute b = tempAL.get(j);
                    if (a.getDataType() == DataType.STRING)            //look to append attributes that hold strings
                    {
                        if (!a.getStringValue().equals(b.getStringValue()))    //if the values are the same for the attributes, nothing is appended
                        {
                            AL.set(j, new Attribute(a.getFullName(), a.getStringValue() + "; " + b.getStringValue()));//append the two attributes
                        }

                    } else {
                        if (!a.getNumericValue().equals(b.getNumericValue()))    //similar procedure above, however the numeric values are preserved as strings if more than one different value exists
                        {
                            AL.set(j, new Attribute(a.getFullName(), a.getNumericValue().toString() + "; " + b.getNumericValue().toString()));
                        }
                           }
                }
            }
            for (Attribute a : AL)                                    //add the new Attribute List to the output file
            {
                outFile.addGlobalAttribute(a);
            }
            //average the variables
            float mean = 0;
            int[] shape;
            int[] origin = null;
            for (String s : fileList) {
                try {
                    myDataFile = NetcdfFile.open(s, null);
                    dataVar = myDataFile.findVariable(var);                //find the variable
                    if (dataVar == null)
                        for (Variable v : myDataFile.getVariables()) {
                            if (v.getShortName().equals(var))
                                dataVar = v;
                        }
                    shape = dataVar.getShape();
                    origin = new int[shape.length];

                    if (shape.length == 1)
                        mean += getMean((D1) dataVar.read(origin, shape));
                    else if (shape.length == 2)
                        mean += getMean((D2) dataVar.read(origin, shape));
                    else if (shape.length == 3)
                        mean += getMean((D3) dataVar.read(origin, shape));
                    else
                        System.out.println("Array data dimensions are not 1 2 or 3!: " + shape.length);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            mean = mean / ((float) fileList.size());        // mean is averaged over all dimensions in all files, divide by number of files

            //The mean is written in two ways, as a global attribute, and as the only data located within a variable under the same name
            Attribute test = new Attribute("Mean of (" + var + ") for " + fileList.toString(), mean);
            outFile.addGlobalAttribute(test);

            //I don't know whether to modify the Dimensions of the variable???
            /*List<Dimension> ld = dataVar.getDimensions();
            for (Dimension aLd : ld) {
                //ld.get(i).setLength(1);
                //ld.get(i).setUnlimited(false);
            }*/

            outFile.addVariable(var, dataVar.getDataType(), dataVar.getDimensions());        //add a new variable modeled after the one being analyzed


            for (Attribute s : dataVar.getAttributes())                                        //add in the attributes for the variable
            {
                if (s.getDataType() == DataType.STRING)
                    outFile.addVariableAttribute(var, s.getShortName(), s.getStringValue());

                else
                    outFile.addVariableAttribute(var, s.getShortName(), s.getNumericValue());
            }

            // create the file; definition phase is over, writing phase begins
            outFile.create();
            D3 mn = new D3(1, 1, 1);                                //write in the data
            mn.set(0, 0, 0, mean);
            outFile.write(var, origin, mn);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != outFile)
                try {
                    outFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }

            if (null != myDataFile)
                try {
                    myDataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        }
    }

    /**
     * averageVariable
     * This function averages a given variable over a list of NetCDF files. A new .txt file is created that preserves the global attributes and dimensions of the given data, but only
     * has the variable being averaged.
     *
     * @param fileList   List of the files to be analyzed
     * @param outputName Name of the output .txt file
     * @param var        variable short name
     */
    public void averageVariableTxt(ArrayList<String> fileList, String outputName, String var) {

        if (outputName.toLowerCase().contains(".txt"))
            outputName = outputName.substring(0, outputName.length() - 4);

        NetcdfFile myDataFile = null;                                //reference file

        try {
            myDataFile = NetcdfFile.open(fileList.get(0), null);


            Variable dataVar = myDataFile.findVariable(var);                //check if the variable is found

            if (dataVar == null)
                for (Variable v : myDataFile.getVariables()) {
                    if (v.getShortName().equals(var))
                        dataVar = v;
                }

            if (dataVar == null) {
                System.out.println("Cannot find Variable data " + var);
                return;
            }

            //average the variables
            float mean = 0;
            int[] shape;
            int[] origin;
            for (String s : fileList) {
                try {
                    myDataFile = NetcdfFile.open(s, null);
                    dataVar = myDataFile.findVariable(var);                //find the variable
                    if (dataVar == null)
                        for (Variable v : myDataFile.getVariables()) {
                            if (v.getShortName().equals(var))
                                dataVar = v;
                        }
                    shape = dataVar.getShape();
                    origin = new int[shape.length];

                    if (shape.length == 1)
                        mean += getMean((D1) dataVar.read(origin, shape));
                    else if (shape.length == 2)
                        mean += getMean((D2) dataVar.read(origin, shape));
                    else if (shape.length == 3)
                        mean += getMean((D3) dataVar.read(origin, shape));
                    else
                        System.out.println("Array data dimensions are not 1 2 or 3!: " + shape.length);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            mean = mean / ((float) fileList.size());

            String units = "";
            for (Attribute a : dataVar.getAttributes()) {
                if (a.getName().toLowerCase().equals("units"))
                    units = a.getStringValue();

                System.out.println(a.toString());
            }


            PrintWriter writer = new PrintWriter(outputName + ".txt", "UTF-8");
            writer.println("File list: " + fileList.toString());
            writer.println("Mean of (" + var + "): " + mean + " " + units);
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != myDataFile)
                try {
                    myDataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        }
    }

    public float findMin(D1 ar) {
        float min = Float.MAX_VALUE;
        int[] shape = ar.getShape();
        for (int i = 0; i < shape[0]; i++) {
            if (min > ar.get(i))
                min = ar.get(i);
        }
        return min;
    }

    public float findMin(D2 ar) {
        float min = Float.MAX_VALUE;
        int[] shape = ar.getShape();
        for (int i = 0; i < shape[0]; i++) {
            for (int j = 0; j < shape[1]; j++) {
                if (min > ar.get(i, j))
                    min = ar.get(i, j);
            }
        }
        return min;
    }

    public float findMin(D3 ar) {
        float min = Float.MAX_VALUE;
        int[] shape = ar.getShape();
        for (int i = 0; i < shape[0]; i++) {
            for (int j = 0; j < shape[1]; j++) {
                for (int k = 0; k < shape[2]; k++) {
                    if (min > ar.get(i, j, k))
                        min = ar.get(i, j, k);
                }
            }
        }
        return min;
    }

    public float findMin(D1 ar, float ignoreValue) {
        float min = Float.MAX_VALUE;
        int[] shape = ar.getShape();
        float temp;
        for (int i = 0; i < shape[0]; i++) {
            temp = ar.get(i);
            if (temp != ignoreValue && min > temp)
                min = temp;
        }
        return min;
    }

    public float findMin(D2 ar, float ignoreValue) {
        float min = Float.MAX_VALUE;
        int[] shape = ar.getShape();
        float temp;
        for (int i = 0; i < shape[0]; i++) {
            for (int j = 0; j < shape[1]; j++) {
                temp = ar.get(i, j);
                if (temp != ignoreValue && min > temp)
                    min = temp;
            }
        }
        return min;
    }

    public float findMin(D3 ar, float ignoreValue) {
        float min = Float.MAX_VALUE;
        int[] shape = ar.getShape();
        float temp;
        for (int i = 0; i < shape[0]; i++) {
            for (int j = 0; j < shape[1]; j++) {
                for (int k = 0; k < shape[2]; k++) {
                    temp = ar.get(i, j, k);
                    if (temp != ignoreValue && min > temp)
                        min = temp;
                }
            }
        }
        return min;
    }

    public float findMin(ArrayList<String> fileList, String var) {
        float min = Float.MAX_VALUE;
        NetcdfFile myDataFile;                                //reference file
        try {
            myDataFile = NetcdfFile.open(fileList.get(0), null);
            Variable dataVar = myDataFile.findVariable(var);                //check if the variable is found

            if (dataVar == null)
                for (Variable v : myDataFile.getVariables()) {
                    if (v.getShortName().equals(var))
                        dataVar = v;
                }
            if (dataVar == null) {
                System.out.println("Cannot find Variable data " + var);
                return min;
            }

            float tempMin = 0;
            for (String s : fileList) {
                myDataFile = NetcdfFile.open(s, null);
                dataVar = myDataFile.findVariable(var);                //check if the variable is found

                if (dataVar == null)
                    for (Variable v : myDataFile.getVariables()) {
                        if (v.getShortName().equals(var))
                            dataVar = v;
                    }
                ArrayFloat ar = (ArrayFloat) dataVar.read();
                if (ar.getShape().length == 1)
                    tempMin = findMin((D1) ar);

                else if (ar.getShape().length == 2)
                    tempMin = findMin((D2) ar);

                else if (ar.getShape().length == 3)
                    tempMin = findMin((D3) ar);

                if (min > tempMin)
                    min = tempMin;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return min;

    }

    public float findMax(D1 ar, float ignoreValue) {
        float max = Float.MIN_VALUE;
        int[] shape = ar.getShape();
        float temp;
        for (int i = 0; i < shape[0]; i++) {
            temp = ar.get(i);
            if (temp != ignoreValue && max < temp)
                max = temp;
        }
        return max;
    }

    public float findMax(D2 ar, float ignoreValue) {
        float max = Float.MIN_VALUE;
        int[] shape = ar.getShape();
        float temp;
        for (int i = 0; i < shape[0]; i++) {
            for (int j = 0; j < shape[1]; j++) {
                temp = ar.get(i, j);
                if (temp != ignoreValue && max < temp)
                    max = temp;
            }
        }
        return max;
    }

    public float findMax(D3 ar, float ignoreValue) {
        float max = Float.MIN_VALUE;
        int[] shape = ar.getShape();
        float temp;
        for (int i = 0; i < shape[0]; i++) {
            for (int j = 0; j < shape[1]; j++) {
                for (int k = 0; k < shape[2]; k++) {
                    temp = ar.get(i, j, k);
                    if (temp != ignoreValue && max < temp)
                        max = temp;
                }
            }
        }
        return max;
    }

    public float findMax(D1 ar) {
        float max = Float.MIN_VALUE;
        int[] shape = ar.getShape();
        for (int i = 0; i < shape[0]; i++) {
            if (max < ar.get(i))
                max = ar.get(i);
        }
        return max;
    }

    public float findMax(D2 ar) {
        float max = Float.MIN_VALUE;
        int[] shape = ar.getShape();
        for (int i = 0; i < shape[0]; i++) {
            for (int j = 0; j < shape[1]; j++) {
                if (max < ar.get(i, j))
                    max = ar.get(i, j);
            }
        }
        return max;
    }

    public float findMax(D3 ar) {
        float max = Float.MIN_VALUE;
        int[] shape = ar.getShape();
        for (int i = 0; i < shape[0]; i++) {
            for (int j = 0; j < shape[1]; j++) {
                for (int k = 0; k < shape[2]; k++) {
                    if (max < ar.get(i, j, k))
                        max = ar.get(i, j, k);
                }
            }
        }
        return max;
    }

    /**
     * findMax
     * Returns a float of the maximum value for a variable within multiple NetCDF files
     *
     * @param fileList array of NetCDFs to open
     * @param var      variable short name
     * @return float of the max value
     */
    public float findMax(ArrayList<String> fileList, String var) {
        float max = Float.MIN_VALUE;
        NetcdfFile myDataFile;                                //reference file
        try {
            myDataFile = NetcdfFile.open(fileList.get(0), null);
            Variable dataVar = myDataFile.findVariable(var);                //check if the variable is found

            if (dataVar == null)
                for (Variable v : myDataFile.getVariables()) {
                    if (v.getShortName().equals(var))
                        dataVar = v;
                }
            if (dataVar == null) {
                System.out.println("Cannot find Variable data " + var);
                return max;
            }

            float tempMax = 0;
            for (String s : fileList) {
                myDataFile = NetcdfFile.open(s, null);
                dataVar = myDataFile.findVariable(var);                //check if the variable is found

                if (dataVar == null)
                    for (Variable v : myDataFile.getVariables()) {
                        if (v.getShortName().equals(var))
                            dataVar = v;
                    }
                ArrayFloat ar = (ArrayFloat) dataVar.read();
                if (ar.getShape().length == 1)
                    tempMax = findMax((D1) ar);

                else if (ar.getShape().length == 2)
                    tempMax = findMax((D2) ar);

                else if (ar.getShape().length == 3)
                    tempMax = findMax((D3) ar);

                if (max < tempMax)
                    max = tempMax;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return max;
    }

    /**
     * makeLegend
     * Create a .png of the legend for a variable for a specified month in one NetCDF file
     *
     * @param fileName   NetCDF file name
     * @param outputName name of output file
     * @param var        variable short name
     */
    public void makeLegend(String fileName, String outputName, String var, String monthName) {
        if (outputName.toLowerCase().contains(".png"))
            outputName = outputName.substring(0, outputName.length() - 4);

        NetcdfFile myDataFile = null;                                //reference file
        try {
            myDataFile = NetcdfFile.open(fileName, null);
            Variable dataVar = myDataFile.findVariable(var);                //check if the variable is found

            if (dataVar == null)
                for (Variable v : myDataFile.getVariables()) {
                    if (v.getShortName().equals(var))
                        dataVar = v;
                }
            if (dataVar == null) {
                System.out.println("Cannot find Variable data " + var);
                return;
            }
            D3 allData = (D3) dataVar.read();
            int month = convertMonthToInt(monthName);
            D2 oneMonth = (D2) allData.slice(0, month);
            float min = findMin(oneMonth);
            float max = findMax(oneMonth);
            PngColor col = new PngColor(min, max);

            col.createLegend(outputName, 510, 60, 10, dataVar.getFullName(), dataVar.getUnitsString());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (myDataFile != null)
                try {
                    myDataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        }
    }

    /**
     * makeLegend
     * Create a .png of the legend for a variable applicable to all variables across all files within the file list
     *
     * @param fileList   array of NetCDFs to open
     * @param outputName name of output file
     * @param var        variable short name
     */
    public void makeLegend(ArrayList<String> fileList, String outputName, String var) {

        if (outputName.toLowerCase().contains(".png"))
            outputName = outputName.substring(0, outputName.length() - 4);

        NetcdfFile myDataFile = null;                                //reference file
        try {
            myDataFile = NetcdfFile.open(fileList.get(0), null);
            Variable dataVar = myDataFile.findVariable(var);                //check if the variable is found

            if (dataVar == null)
                for (Variable v : myDataFile.getVariables()) {
                    if (v.getShortName().equals(var))
                        dataVar = v;
                }
            if (dataVar == null) {
                System.out.println("Cannot find Variable data " + var);
                return;
            }

            float min = findMin(fileList, var);
            float max = findMax(fileList, var);
            PngColor col = new PngColor(min, max);

            col.createLegend(outputName, 510, 60, 10, dataVar.getDescription(), dataVar.getUnitsString());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (myDataFile != null)
                try {
                    myDataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        }
    }
    /**
     * Zhenlong
     * makeChart
     * Creates a .png of interpolated data for one month with the option to create a separate legend image for the map. The interpolated data array is formed using Inverse distance weighting.
     *
     * @param fileName        which NetCDF to use
     * @param outputName      name of output file
     * @param var             variable short name
     * @param month           which month to use (0-11)
     * @param scale           image resolution is determined by data dimensions * scale
     * @param thresholdFactor affects color blending of the image, each interpolated point is calculated within a radius for original data points that scales with this value.
     * @param createLegend    true to create a legend, false to only create the map image
     */
	public void makeChartHDFS(float[][] grid, String outputName, String var,
			float minValue, float maxValue, float fillValue,
			boolean createLegend, boolean displayLabel, String label) {

		try {

			PngColor color;
			int[][] rgb;
			color = new PngColor(minValue, maxValue); // create the png color
														// with the dimensions
														// for the legend
			// D2 idw = interpolateArray(oneMonth, scale, thresholdFactor, mv);
			// //interpolate the data
			// shape2d = idw.getShape();

			int row = grid.length;
			int col = grid[0].length;
			rgb = new int[col][row]; // since the dimensions are latxlong, flip
										// them in the png color array to
										// display the map in the correct
										// orientation

			float val;
			for (int i = 0; i < col; i++) {
				for (int j = 0; j < row; j++) {
					val = grid[row - 1 - j][i];
					if (true/*!Helper.isFillValue(val)*/)//if (val != fillValue)
						rgb[i][j] = color.getColorRGB(val);
					else
						rgb[i][j] = -1;
				}
			}

			MutableImage mi = new MutableImage(rgb, col, row);

			if (displayLabel) { // add the date display
				//Calendar c = Calendar.getInstance();
				//c.setTime(date);
				mi.drawString(label, 0, 0, font,Color.DARK_GRAY);
			}

			if (createLegend) // add the legend
				mi.combineImagesVertical(mi.getImage(),color.getLegendImage(col, 60, 28, var, ""));
			
			//output to HDFS
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", MyProperty.nameNode);
			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
			conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
			FileSystem fs=null;
			try {
				fs = FileSystem.get(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			BufferedImage originalImage = mi.getImage();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ImageIO.write( originalImage, "png", baos );
			baos.flush();
			byte[] imageInByte = baos.toByteArray();
			
			FSDataOutputStream hdfsWriter = fs.create(new Path(outputName));
			hdfsWriter.write(imageInByte);
			hdfsWriter.flush();		 
			hdfsWriter.close(); 
			
			baos.close();
		    
			

			//Output to local:
			//PngWriter png = new PngWriter(outputName, mi); // create the png
			//png.createMutableImage();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
     * Zhenlong
     * makeChart
     * Creates a .png of interpolated data for one month with the option to create a separate legend image for the map. The interpolated data array is formed using Inverse distance weighting.
     *
     * @param fileName        which NetCDF to use
     * @param outputName      name of output file
     * @param var             variable short name
     * @param month           which month to use (0-11)
     * @param scale           image resolution is determined by data dimensions * scale
     * @param thresholdFactor affects color blending of the image, each interpolated point is calculated within a radius for original data points that scales with this value.
     * @param createLegend    true to create a legend, false to only create the map image
     */
	public void makeChart(float[][] grid, String outputName, String var,
			float minValue, float maxValue, float fillValue,
			boolean createLegend, boolean displayLabel, String label) {

	if (outputName.toLowerCase().contains(".png"))
			outputName = outputName.substring(0, outputName.length() - 4);
		try {

			PngColor color;
			int[][] rgb;
			color = new PngColor(minValue, maxValue); // create the png color
														// with the dimensions
														// for the legend
			// D2 idw = interpolateArray(oneMonth, scale, thresholdFactor, mv);
			// //interpolate the data
			// shape2d = idw.getShape();

			int row = grid.length;
			int col = grid[0].length;
			rgb = new int[col][row]; // since the dimensions are latxlong, flip
										// them in the png color array to
										// display the map in the correct
										// orientation

			float val;
			for (int i = 0; i < col; i++) {
				for (int j = 0; j < row; j++) {
					val = grid[row - 1 - j][i];
					if (true/*!Helper.isFillValue(val)*/)//if (val != fillValue)
						rgb[i][j] = color.getColorRGB(val);
					else
						rgb[i][j] = -1;
				}
			}

			MutableImage mi = new MutableImage(rgb, col, row);

			if (displayLabel) { // add the date display
				//Calendar c = Calendar.getInstance();
				//c.setTime(date);
				mi.drawString(label, 0, 0, font,Color.DARK_GRAY);
			}

			if (createLegend) // add the legend
				mi.combineImagesVertical(mi.getImage(),color.getLegendImage(col, 60, 28, var, ""));
			
			 

			//Output to local:
			PngWriter png = new PngWriter(outputName, mi); // create the png
			png.createMutableImage();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
    /**
     * makeChart
     * Creates a .png of interpolated data for one month with the option to create a separate legend image for the map. The interpolated data array is formed using Inverse distance weighting.
     *
     * @param fileName        which NetCDF to use
     * @param outputName      name of output file
     * @param var             variable short name
     * @param month           which month to use (0-11)
     * @param scale           image resolution is determined by data dimensions * scale
     * @param thresholdFactor affects color blending of the image, each interpolated point is calculated within a radius for original data points that scales with this value.
     * @param createLegend    true to create a legend, false to only create the map image
     */
    public void makeChart(String fileName, String outputName, String var, int month, int scale, float thresholdFactor, boolean createLegend, boolean displayLabel, Date date) {
        NetcdfFile myDataFile = null;                                            //reference file

        if (outputName.toLowerCase().contains(".png"))
            outputName = outputName.substring(0, outputName.length() - 4);

        try {
            myDataFile = NetcdfFile.open(fileName, null);
            Variable dataVar = myDataFile.findVariable(var);                //check if the variable is found

            if (dataVar == null)
                for (Variable v : myDataFile.getVariables()) {
                    if (v.getShortName().equals(var))
                        dataVar = v;
                }
            if (dataVar == null) {
                System.out.println("Cannot find Variable data " + var);
                return;
            }

            int[] shape = dataVar.getShape();
            int[] origin = new int[shape.length];
            D3 dataArray = (D3) dataVar.read(origin, shape);             //read in the data
            D2 oneMonth;
            try {
                oneMonth = (D2) dataArray.slice(0, month);                //use the month given
            } catch (Exception e) {
                System.out.println("ERROR Invalid month: " + month);
                e.printStackTrace();
                return;
            }

            Attribute missingValue = myDataFile.findGlobalAttribute("missing_value");   //see if there is a global attribute for the missing values

            float min, max;
            PngColor col;
            int[] shape2d;
            int[][] rgb;

            if (missingValue == null) { //continue normally
                min = findMin(oneMonth);
                max = findMax(oneMonth);
                col = new PngColor(min, max);                          //create the png color with the dimensions for the legend
                D2 idw = interpolateArray(oneMonth, scale, thresholdFactor); //interpolate the data
                shape2d = idw.getShape();

                rgb = new int[shape2d[1]][shape2d[0]];                          //since the dimensions are latxlong, flip them in the png color array to display the map in the correct orientation
                for (int i = 0; i < shape2d[1]; i++) {
                    for (int j = 0; j < shape2d[0]; j++) {
                        rgb[i][j] = col.getColorRGB(idw.get((shape2d[0] - 1) - j, i));
                    }
                }
            } else {    //missing values will be ignored in interpolation
                float mv = missingValue.getNumericValue().floatValue();
                min = findMin(oneMonth, mv);
                max = findMax(oneMonth, mv);
                col = new PngColor(min, max);                          //create the png color with the dimensions for the legend
                D2 idw = interpolateArray(oneMonth, scale, thresholdFactor, mv); //interpolate the data
                shape2d = idw.getShape();

                rgb = new int[shape2d[1]][shape2d[0]];                          //since the dimensions are latxlong, flip them in the png color array to display the map in the correct orientation
                float val;
                for (int i = 0; i < shape2d[1]; i++) {
                    for (int j = 0; j < shape2d[0]; j++) {
                        val = idw.get((shape2d[0] - 1) - j, i);
                        if (val != mv)
                            rgb[i][j] = col.getColorRGB(val);
                        else
                            rgb[i][j] = -1;
                    }
                }
            }
            MutableImage mi = new MutableImage(rgb, shape2d[1], shape2d[0]);

            if (displayLabel)                                                    //add the date display
            {
                Calendar c = Calendar.getInstance();
                c.setTime(date);
                mi.drawString(df.format(c.getTime()), 0, 0, font, Color.DARK_GRAY);
            }

            if (createLegend)                                //add the legend
                mi.combineImagesVertical(mi.getImage(), col.getLegendImage(shape2d[1], 60, 28, dataVar.getDescription(), dataVar.getUnitsString()));

            PngWriter png = new PngWriter(outputName, mi);    //create the png
            png.createMutableImage();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (myDataFile != null)
                try {
                    myDataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        }
    }

    /**
     * makeChart
     * makeChart for multiple charts using the same legend, so that a single legend can be created for multiple images using specified min and max values
     *
     * @param fileName        which NetCDF to use
     * @param outputName      name of output file
     * @param var             variable short name
     * @param month           which month to use (0-11)
     * @param scale           image resolution is determined by data dimensions * scale
     * @param thresholdFactor affects color blending of the image, each interpolated point is calculated within a radius for original data points that scales with this value (recommended: 2f)
     * @param createLegend    true to create a legend, false to only create the map image
     * @param displayLabel    add a display date on the top left?
     * @param date            date to display
     * @param min             min value
     * @param max             max value
     */
    public void makeChart(String fileName, String outputName, String var, int month, int scale, float thresholdFactor, boolean createLegend, boolean displayLabel, Date date, float min, float max) {
        NetcdfFile myDataFile = null;                                            //reference file

        if (outputName.toLowerCase().contains(".png"))
            outputName = outputName.substring(0, outputName.length() - 4);

        try {
            myDataFile = NetcdfFile.open(fileName, null);
            Variable dataVar = myDataFile.findVariable(var);                //check if the variable is found

            if (dataVar == null)
                for (Variable v : myDataFile.getVariables()) {
                    if (v.getShortName().equals(var))
                        dataVar = v;
                }
            if (dataVar == null) {
                System.out.println("Cannot find Variable data " + var);
                return;
            }

            int[] shape = dataVar.getShape();
            int[] origin = new int[shape.length];
            D3 dataArray = (D3) dataVar.read(origin, shape);  //read in the data
            D2 oneMonth = (D2) dataArray.slice(0, month);                //use the month given

            PngColor col = new PngColor(min, max);


            D2 idw = interpolateArray(oneMonth, scale, thresholdFactor); //interpolate the data
            int[] shape2d = idw.getShape();

            int[][] rgb = new int[shape2d[1]][shape2d[0]];                          //since the dimensions are latxlong, flip them in the png color array to display the map in the correct orientation
            for (int i = 0; i < shape2d[1]; i++) {
                for (int j = 0; j < shape2d[0]; j++) {
                    rgb[i][j] = col.getColorRGB(idw.get((shape2d[0] - 1) - j, i));
                }
            }

            MutableImage mi = new MutableImage(rgb, shape2d[1], shape2d[0]);

            if (displayLabel)                                                    //add the date display
            {
                Calendar c = Calendar.getInstance();
                c.setTime(date);
                mi.drawString(df.format(c.getTime()), 0, 0, font, Color.DARK_GRAY);
            }

            if (createLegend)                                //add the legend
                mi.combineImagesVertical(mi.getImage(), col.getLegendImage(shape2d[1], 60, 28, dataVar.getDescription(), dataVar.getUnitsString()));

            PngWriter png = new PngWriter(outputName, mi);    //create the png
            png.createMutableImage();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (myDataFile != null)
                try {
                    myDataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }

        }
    }

    /**
     * makeChartBilinearForEachVar
     * prints out a .png chart for each variable within a NetCDF file
     *
     * @param fileName
     * @param outputName
     * @param scale
     * @param createLegend
     * @param displayLabel
     * @param date
     */
    public void makeChartBilinearForEachVar(String fileName, String outputName, int scale, boolean createLegend, boolean displayLabel, Date date) {
        NetcdfFile nc;
        try {
            nc = NetcdfFile.open(fileName);
            for (Variable v : nc.getVariables()) {
                if (v.read().getShape().length > 2)
                    makeChartBilinear(fileName, outputName + "_" + v.getDescription(), v.getShortName(), 0, scale, createLegend, displayLabel, date);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * makeCharts
     * Creates a .png for every month of a variable in every file in fileList and options on how to create the legends
     *
     * @param fileList        which NetCDF to use
     * @param outputName      name of output file
     * @param var             variable short name
     * @param scale           image resolution is determined by data dimensions * scale
     * @param thresholdFactor affects color blending of the image, each interpolated point is calculated within a radius for original data points that scales with this value (recommended: 2f)
     * @param createLegend    true to create a legend, false to only create the map image
     * @param singleLegend    true to create one legend for all the images, false to create a legend for each individual image
     */
    public void makeCharts(ArrayList<String> fileList, String outputName, String var, int scale, float thresholdFactor, boolean createLegend, boolean singleLegend, boolean displayLabel, Date startDate) {
        if (outputName.toLowerCase().contains(".png"))
            outputName = outputName.substring(0, outputName.length() - 4);

        int time = getTimeLength(fileList.get(0));      //determine the time dimension length

        Calendar c = Calendar.getInstance();
        if (displayLabel)
            c.setTime(startDate);

        if (createLegend && singleLegend)        //make one legend for all the images
        {
            float min = findMin(fileList, var);
            float max = findMax(fileList, var);
            for (String s : fileList) {
                for (int i = 0; i < time; i++) {
                    String monthName = convertIntToMonth(i);
                    makeChart(s, outputName + "_" + var + "_" + s + "_" + monthName, var, i, scale, thresholdFactor, true, displayLabel, c.getTime(), min, max);
                    c.add(Calendar.MONTH, 1);
                }
            }
        } else if (createLegend && !singleLegend)  //make a legend for every image
        {
            for (String s : fileList) {
                for (int i = 0; i < time; i++) {
                    String monthName = convertIntToMonth(i);
                    makeChart(s, outputName + "_" + var + "_" + s + "_" + monthName, var, i, scale, thresholdFactor, true, displayLabel, c.getTime());
                    c.add(Calendar.MONTH, 1);
                }
            }
        } else if (!createLegend)      //create the images without any legend
        {
            for (String s : fileList) {
                for (int i = 0; i < time; i++) {
                    String monthName = convertIntToMonth(i);
                    makeChart(s, outputName + "_" + var + "_" + s + "_" + monthName, var, i, scale, thresholdFactor, false, displayLabel, c.getTime());
                    c.add(Calendar.MONTH, 1);
                }
            }
        }
    }

    public void modifyArray(D2 ar, float target, float replacement) {
        int[] shape = ar.getShape();
        for (int i = 0; i < shape[0]; i++) {
            for (int j = 0; j < shape[1]; j++) {
                if (ar.get(i, j) == target)
                    ar.set(i, j, replacement);
            }
        }
    }


    /**
     * makeChartBilinear
     * creates a .png of a variable for one month within a NetCDF file, includes the option to display a legend and a date. Uses Bilinear Interpolation for scaling the image
     *
     * @param fileName     file path of NetCDF file
     * @param outputName   name of output file
     * @param var          variable short name
     * @param month        which month to use (0-11)
     * @param scale        image resolution is determined by data dimensions * scale
     * @param createLegend add a legend to this chart?
     * @param displayLabel add a display date on the top left?
     * @param date         date to display
     */
    public void makeChartBilinear(String fileName, String outputName, String var, int month, int scale, boolean createLegend, boolean displayLabel, Date date) {
        NetcdfFile myDataFile = null;                                            //reference file

        if (outputName.toLowerCase().contains(".png"))
            outputName = outputName.substring(0, outputName.length() - 4);

        try {
            myDataFile = NetcdfFile.open(fileName, null);
            Variable dataVar = myDataFile.findVariable(var);                //check if the variable is found

            if (dataVar == null)
                for (Variable v : myDataFile.getVariables()) {
                    if (v.getShortName().equals(var))
                        dataVar = v;
                }
            if (dataVar == null) {
                System.out.println("Cannot find Variable data " + var);
                return;
            }

            int[] shape = dataVar.getShape();
            int[] origin = new int[shape.length];
            D3 dataArray = (D3) dataVar.read(origin, shape);             //read in the data
            D2 oneMonth;
            try {
                oneMonth = (D2) dataArray.slice(0, month);                //use the month given
            } catch (Exception e) {
                System.out.println("ERROR Invalid month given: " + month);
                e.printStackTrace();
                return;
            }

            Attribute missingValue = myDataFile.findGlobalAttribute("missing_value");   //see if there is a global attribute for the missing values

            float min, max;
            PngColor col;
            int[] shape2d = oneMonth.getShape();
            int[][] rgb = new int[shape2d[1]][shape2d[0]];                          //since the dimensions are latxlong, flip them in the png color array to display the map in the correct orientation

            if (missingValue == null) { //not found, continue normally
                min = findMin(oneMonth);
                max = findMax(oneMonth);
                col = new PngColor(min, max);

                for (int i = 0; i < shape2d[1]; i++) {
                    for (int j = 0; j < shape2d[0]; j++) {
                        rgb[i][j] = col.getColorRGB(oneMonth.get((shape2d[0] - 1) - j, i));
                    }
                }
            } else {                     //work with missing values
                float mv = missingValue.getNumericValue().floatValue();
                min = findMin(oneMonth, mv);        //find min and max but ignore the missing values
                max = findMax(oneMonth, mv);

                col = new PngColor(min, max);

                float val;
                for (int i = 0; i < shape2d[1]; i++) {
                    for (int j = 0; j < shape2d[0]; j++) {
                        val = oneMonth.get((shape2d[0] - 1) - j, i);
                        if (val != mv)
                            rgb[i][j] = col.getColorRGB(oneMonth.get((shape2d[0] - 1) - j, i));

                        else
                            rgb[i][j] = -1;
                    }
                }
            }

            MutableImage mi = new MutableImage(rgb, shape2d[1], shape2d[0]);
            mi.setImage(mi.resizeBilinear(mi.getImage(), shape2d[1] * scale, shape2d[0] * scale));

            if (displayLabel) {//add the date display
                Calendar c = Calendar.getInstance();
                c.setTime(date);
                mi.drawString(df.format(c.getTime()), 0, 0, font, Color.DARK_GRAY);
            }
            if (createLegend)
                mi.combineImagesVertical(mi.getImage(), col.getLegendImage(shape2d[1] * scale, 60, 28, dataVar.getDescription(), dataVar.getUnitsString()));

            PngWriter png = new PngWriter(outputName, mi);    //create the png
            png.createMutableImage();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (myDataFile != null)
                try {
                    myDataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        }
    }

    /**
     * makeChartBicubic
     * creates a .png of a variable for one month within a NetCDF file, includes the option to display a legend and a date. Uses Bicubic Interpolation for scaling the image
     *
     * @param fileName     file path of NetCDF file
     * @param outputName   name of output file
     * @param var          variable short name
     * @param month        which month to use (0-11)
     * @param scale        image resolution is determined by data dimensions * scale
     * @param createLegend add a legend to this chart?
     * @param displayLabel add a display date on the top left?
     * @param date         date to display
     */
    public void makeChartBicubic(String fileName, String outputName, String var, int month, int scale, boolean createLegend, boolean displayLabel, Date date) {
        NetcdfFile myDataFile = null;                                       //open the NetCDF file

        if (outputName.toLowerCase().contains(".png"))
            outputName = outputName.substring(0, outputName.length() - 4);

        try {
            myDataFile = NetcdfFile.open(fileName, null);
            Variable dataVar = myDataFile.findVariable(var);                //check if the variable is found

            if (dataVar == null)
                for (Variable v : myDataFile.getVariables()) {
                    if (v.getShortName().equals(var))
                        dataVar = v;
                }
            if (dataVar == null) {
                System.out.println("Cannot find Variable data " + var);
                return;
            }

            int[] shape = dataVar.getShape();
            int[] origin = new int[shape.length];
            D3 dataArray = (D3) dataVar.read(origin, shape);                //read in the data
            D2 oneMonth;
            try {
                oneMonth = (D2) dataArray.slice(0, month);                //use the month given
            } catch (Exception e) {
                System.out.println("ERROR Invalid month given: " + month);
                e.printStackTrace();
                return;
            }
            float min = findMin(oneMonth);
            float max = findMax(oneMonth);
            PngColor col = new PngColor(min, max);                          //create the color based on the min and max from one month of data

            int[] shape2d = oneMonth.getShape();

            int[][] rgb = new int[shape2d[1]][shape2d[0]];                  //since the dimensions are latxlong, flip them in the png color array to display the map in the correct orientation
            for (int i = 0; i < shape2d[1]; i++) {
                for (int j = 0; j < shape2d[0]; j++) {
                    rgb[i][j] = col.getColorRGB(oneMonth.get((shape2d[0] - 1) - j, i));
                }
            }

            MutableImage mi = new MutableImage(rgb, shape2d[1], shape2d[0]);
            mi.setImage(mi.resizeBicubic(mi.getImage(), shape2d[1] * scale, shape2d[0] * scale));

            if (displayLabel)            //add the date display
            {
                Calendar c = Calendar.getInstance();
                c.setTime(date);
                mi.drawString(df.format(c.getTime()), 0, 0, font, Color.DARK_GRAY);
            }

            /* if (createLegend)
                col.createLegend(outputName + " legend",510,60,10, dataVar.getDescription(), dataVar.getUnitsString());                   //create the legend .png
            */
            if (createLegend)
                mi.combineImagesVertical(mi.getImage(), col.getLegendImage(shape2d[1] * scale, 60, 28, dataVar.getDescription(), dataVar.getUnitsString()));

            PngWriter png = new PngWriter(outputName, mi);    //create the png
            png.createMutableImage();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (myDataFile != null)
                try {
                    myDataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        }
    }

    /**
     * makeChartNearestNeighbor
     * creates a .png of a variable for one month within a NetCDF file, includes the option to display a legend and a date. Uses NearestNeighbor Interpolation for scaling the image
     *
     * @param fileName     file path of NetCDF file
     * @param outputName   name of output file
     * @param var          variable short name
     * @param month        which month to use (0-11)
     * @param scale        image resolution is determined by data dimensions * scale
     * @param createLegend add a legend to this chart?
     * @param displayLabel add a display date on the top left?
     * @param date         date to display
     */
    public void makeChartNearestNeighbor(String fileName, String outputName, String var, int month, int scale, boolean createLegend, boolean displayLabel, Date date) {
        NetcdfFile myDataFile = null;                                            //reference file

        if (outputName.toLowerCase().contains(".png"))
            outputName = outputName.substring(0, outputName.length() - 4);

        try {
            myDataFile = NetcdfFile.open(fileName, null);
            Variable dataVar = myDataFile.findVariable(var);                //check if the variable is found

            if (dataVar == null)
                for (Variable v : myDataFile.getVariables()) {
                    if (v.getShortName().equals(var))
                        dataVar = v;
                }
            if (dataVar == null) {
                System.out.println("Cannot find Variable data " + var);
                return;
            }

            int[] shape = dataVar.getShape();
            int[] origin = new int[shape.length];
            D3 dataArray = (D3) dataVar.read(origin, shape);             //read in the data
            D2 oneMonth;
            try {
                oneMonth = (D2) dataArray.slice(0, month);                //use the month given
            } catch (Exception e) {
                System.out.println("ERROR Invalid month given: " + month);
                e.printStackTrace();
                return;
            }
            float min = findMin(oneMonth);
            float max = findMax(oneMonth);
            PngColor col = new PngColor(min, max);

            int[] shape2d = oneMonth.getShape();

            int[][] rgb = new int[shape2d[1]][shape2d[0]];                          //since the dimensions are latxlong, flip them in the png color array to display the map in the correct orientation
            for (int i = 0; i < shape2d[1]; i++) {
                for (int j = 0; j < shape2d[0]; j++) {
                    rgb[i][j] = col.getColorRGB(oneMonth.get((shape2d[0] - 1) - j, i));
                }
            }

            MutableImage mi = new MutableImage(rgb, shape2d[1], shape2d[0]);
            mi.setImage(mi.resizeNearestNeighbor(mi.getImage(), shape2d[1] * scale, shape2d[0] * scale));

            if (displayLabel)            //add the date display
            {
                Calendar c = Calendar.getInstance();
                c.setTime(date);
                mi.drawString(df.format(c.getTime()), 0, 0, font, Color.DARK_GRAY);
            }

            /* if (createLegend)
                col.createLegend(outputName + " legend",510,60,10, dataVar.getDescription(), dataVar.getUnitsString());                   //create the legend .png
            */
            if (createLegend)
                mi.combineImagesVertical(mi.getImage(), col.getLegendImage(shape2d[1] * scale, 60, 28, dataVar.getDescription(), dataVar.getUnitsString()));

            PngWriter png = new PngWriter(outputName, mi);    //create the png
            png.createMutableImage();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (myDataFile != null)
                try {
                    myDataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }

        }
    }

    /**
     * getTimeLength
     * searches a NetCDF file for the length of its time dimension, defaults 1 if a time dimension cannot be found
     *
     * @param fileName file path of NetCDF file
     * @return int value of the time length
     */
    public int getTimeLength(String fileName) {
        int t = 1;
        try {
            NetcdfFile myFile = NetcdfFile.open(fileName);
            Dimension time = null;
            for (Dimension d : myFile.getDimensions()) {
                if (d.getName().toLowerCase().contains("time"))
                    time = d;
            }
            if (time != null)
                t = time.getLength();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return t;
    }


    /**
     * makeGifOfVariableBilinear
     * creates an animated .gif file based on one variable's data within one NetCDF file using Bilinear interpolation to scale the image up. This implementation computes faster than the IDW makeGif function, but the image loses a complete gradient
     *
     * @param fileName     file path of NetCDF file
     * @param var          variable short name
     * @param outputName   name of output gif
     * @param scale        image resolution is determined by data dimensions * scale
     * @param gifInterval  delay of the gif animation in ms
     * @param createLegend add a legend to this chart?
     * @param displayLabel add a display date on the top left?
     * @param startDate    when to start the display date
     */
    public void makeGifOfVariableBilinear(String fileName, String outputName, String var, int scale, int gifInterval, boolean createLegend, boolean displayLabel, Date startDate) {
        if (outputName.toLowerCase().contains(".gif"))
            outputName = outputName.substring(0, outputName.length() - 4);

        ArrayList<Image> images = new ArrayList<Image>();       //load all the data into images to be put into a gif
        NetcdfFile myDataFile;
        try {
            myDataFile = NetcdfFile.open(fileName);
            Variable dataVar = myDataFile.findVariable(var);                //check if the variable is found

            if (dataVar == null)
                for (Variable v : myDataFile.getVariables()) {
                    if (v.getShortName().equals(var))
                        dataVar = v;
                }
            if (dataVar == null) {
                System.out.println("Cannot find Variable data " + var);
                return;
            }

            int time = getTimeLength(fileName);                         //determine the dimensions of the data array
            int[] shape = dataVar.getShape();
            int[] origin = new int[shape.length];
            D3 dataArray = (D3) dataVar.read(origin, shape);             //read in the data

            Attribute missingValue = myDataFile.findGlobalAttribute("missing_value");   //see if there is a global attribute for the missing values

            PngColor col;
            float min;                              //min and max for the legend and chart displays
            float max;

            if (missingValue == null) {   // no missing value attribute found so continue normally
                min = findMin(dataArray);
                max = findMax(dataArray);
                col = new PngColor(min, max);

                Calendar c = Calendar.getInstance();
                c.setTime(startDate);
                //create the colors for each month and add them to the images list
                for (int k = 0; k < time; k++) {
                    BufferedImage buffer = new BufferedImage(shape[2], shape[1], BufferedImage.TYPE_INT_ARGB);
                    Graphics2D g2D = buffer.createGraphics();
                    g2D.setComposite(AlphaComposite.getInstance(AlphaComposite.CLEAR, 0.0f));
                    Rectangle2D.Double rect = new Rectangle2D.Double(0, 0, shape[2], shape[1]);
                    g2D.fill(rect);

                    for (int i = 0; i < shape[2]; i++) {
                        for (int j = 0; j < shape[1]; j++) {
                            buffer.setRGB(i, j, col.getColorRGB(dataArray.get(k, (shape[1] - 1) - j, i)));
                        }
                    }

                    int w = buffer.getWidth(null);
                    int h = buffer.getHeight(null);

                    BufferedImage bilinear = new BufferedImage(scale * w, scale * h,
                            BufferedImage.TYPE_INT_ARGB);

                    Graphics2D bg = bilinear.createGraphics();
                    bg.setRenderingHint(RenderingHints.KEY_INTERPOLATION,
                            RenderingHints.VALUE_INTERPOLATION_BILINEAR);

                    bg.setRenderingHint(RenderingHints.KEY_RENDERING,
                            RenderingHints.VALUE_RENDER_QUALITY);

                    bg.setRenderingHint(
                            RenderingHints.KEY_ANTIALIASING,
                            RenderingHints.VALUE_ANTIALIAS_ON);

                    bg.drawImage(buffer, 0, 0, w * scale, h * scale, null);
                    if (displayLabel) {
                        bg.setComposite(AlphaComposite.Src);

                        bg.setFont(font);
                        bg.setColor(Color.DARK_GRAY);
                        FontMetrics fm = bg.getFontMetrics();
                        bg.drawString(df.format(c.getTime()), 0, fm.getAscent());
                        c.add(Calendar.MONTH, 1);
                    }
                    bg.dispose();

                    images.add(bilinear);
                }

                if (createLegend) {
                    MutableImage myi = new MutableImage();
                    int legendWidth = shape[2] * scale;
                    int legendHeight = 60;
                    if (legendWidth < 200)
                        legendWidth = 510;

                    for (int i = 0; i < images.size(); i++) {

                        myi.combineImagesVertical((BufferedImage) images.get(i), col.getLegendImage(legendWidth, legendHeight, 28, dataVar.getDescription(), dataVar.getUnitsString()));
                        images.set(i, myi.getImage());
                    }
                }
            } else {  //missing value attribute found
                float mv = missingValue.getNumericValue().floatValue();
                min = findMin(dataArray, mv);
                max = findMax(dataArray, mv);
                col = new PngColor(min, max);

                Calendar c = Calendar.getInstance();
                c.setTime(startDate);
                //create the colors for each month and add them to the images list
                float val;
                for (int k = 0; k < time; k++) {
                    BufferedImage buffer = new BufferedImage(shape[2], shape[1], BufferedImage.TYPE_INT_ARGB);
                    Graphics2D g2D = buffer.createGraphics();
                    g2D.setComposite(AlphaComposite.getInstance(AlphaComposite.CLEAR, 0.0f));
                    Rectangle2D.Double rect = new Rectangle2D.Double(0, 0, shape[2], shape[1]);
                    g2D.fill(rect);

                    for (int i = 0; i < shape[2]; i++) {
                        for (int j = 0; j < shape[1]; j++) {
                            val = dataArray.get(k, (shape[1] - 1) - j, i);
                            if (val != mv)
                                buffer.setRGB(i, j, col.getColorRGB(val));

                            else
                                buffer.setRGB(i, j, -1);
                        }
                    }

                    int w = buffer.getWidth(null);
                    int h = buffer.getHeight(null);

                    BufferedImage bilinear = new BufferedImage(scale * w, scale * h,
                            BufferedImage.TYPE_INT_ARGB);

                    Graphics2D bg = bilinear.createGraphics();
                    bg.setRenderingHint(RenderingHints.KEY_INTERPOLATION,
                            RenderingHints.VALUE_INTERPOLATION_BILINEAR);

                    bg.setRenderingHint(RenderingHints.KEY_RENDERING,
                            RenderingHints.VALUE_RENDER_QUALITY);

                    bg.setRenderingHint(
                            RenderingHints.KEY_ANTIALIASING,
                            RenderingHints.VALUE_ANTIALIAS_ON);

                    bg.drawImage(buffer, 0, 0, w * scale, h * scale, null);
                    if (displayLabel) {
                        bg.setComposite(AlphaComposite.Src);

                        bg.setFont(font);
                        bg.setColor(Color.DARK_GRAY);
                        FontMetrics fm = bg.getFontMetrics();
                        bg.drawString(df.format(c.getTime()), 0, fm.getAscent());
                        c.add(Calendar.MONTH, 1);
                    }
                    bg.dispose();

                    images.add(bilinear);
                }

                if (createLegend) {
                    MutableImage myi = new MutableImage();
                    int legendWidth = shape[2] * scale;
                    int legendHeight = 60;
                    if (legendWidth < 200)
                        legendWidth = 510;

                    for (int i = 0; i < images.size(); i++) {

                        myi.combineImagesVertical((BufferedImage) images.get(i), col.getLegendImage(legendWidth, legendHeight, 28, dataVar.getDescription(), dataVar.getUnitsString()));
                        images.set(i, myi.getImage());
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        // grab the output image type from the first image in the sequence
        try {
            BufferedImage firstImage = (BufferedImage) images.get(0);

            // create a new BufferedOutputStream with the last argument
            ImageOutputStream output =
                    new FileImageOutputStream(new File(outputName + ".gif"));

            // create a gif sequence with the type of the first image, 1 second
            // between frames, which loops continuously
            GifSequenceWriter writer =
                    new GifSequenceWriter(output, firstImage.getType(), gifInterval, true);

            // write out the first image to our sequence...
            writer.writeToSequence(firstImage);
            for (int i = 1; i < images.size(); i++) {
                BufferedImage nextImage = (BufferedImage) images.get(i);
                writer.writeToSequence(nextImage);
            }
            writer.close();
            output.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * makeGifOfVariable
     * creates an animated .gif file based on one variable's data within one NetCDF file using IDW interpolation to scale the image up. This implementation computes slower than the Bilinear makeGif function, but the image has a complete gradient
     *
     * @param fileName     file path of NetCDF file
     * @param var          variable short name
     * @param outputName   name of output gif
     * @param scale        image resolution is determined by data dimensions * scale
     * @param gifInterval  delay of the gif animation in ms
     * @param createLegend add a legend to this chart?
     * @param displayLabel add a display date on the top left?
     * @param startDate    when to start the display date
     */
    public void makeGifOfVariable(String fileName, String var, String outputName, int scale, int gifInterval, boolean createLegend, boolean displayLabel, Date startDate) {
        if (outputName.toLowerCase().contains(".gif"))
            outputName = outputName.substring(0, outputName.length() - 4);

        ArrayList<Image> images = new ArrayList<Image>();       //load all the data into images to be put into a gif
        NetcdfFile myDataFile;
        try {
            myDataFile = NetcdfFile.open(fileName);
            Variable dataVar = myDataFile.findVariable(var);                //check if the variable is found

            if (dataVar == null)
                for (Variable v : myDataFile.getVariables()) {
                    if (v.getShortName().equals(var))
                        dataVar = v;
                }
            if (dataVar == null) {
                System.out.println("Cannot find Variable data " + var);
                return;
            }

            int time = getTimeLength(fileName);                         //determine the dimensions of the data array
            int[] shape = dataVar.getShape();
            int[] origin = new int[shape.length];
            D3 dataArray = (D3) dataVar.read(origin, shape);             //read in the data

            float min = findMin(dataArray);                              //min and max for the legend and chart displays
            float max = findMax(dataArray);

            if (scale > 1) {
                dataArray = interpolateArray(dataArray, scale, 2f);         //scale the data if necessary with idw interpolation
                for (int i = 0; i < shape.length; i++)
                    shape[i] = shape[i] * scale;
            }

            PngColor col = new PngColor(min, max);
            Calendar c = Calendar.getInstance();
            c.setTime(startDate);
            //create the colors for each month and add them to the images list
            for (int k = 0; k < time; k++) {
                BufferedImage buffer = new BufferedImage(shape[2], shape[1], BufferedImage.TYPE_INT_ARGB);
                Graphics2D g2D = buffer.createGraphics();
                g2D.setComposite(AlphaComposite.getInstance(AlphaComposite.CLEAR, 0.0f));
                Rectangle2D.Double rect = new Rectangle2D.Double(0, 0, shape[2], shape[1]);
                g2D.fill(rect);

                for (int i = 0; i < shape[2]; i++) {
                    for (int j = 0; j < shape[1]; j++) {
                        buffer.setRGB(i, j, col.getColorRGB(dataArray.get(k, (shape[1] - 1) - j, i)));
                    }
                }
                if (displayLabel) {
                    g2D.setComposite(AlphaComposite.Src);
                    g2D.setRenderingHint(
                            RenderingHints.KEY_ANTIALIASING,
                            RenderingHints.VALUE_ANTIALIAS_ON);
                    g2D.setFont(font);
                    g2D.setColor(Color.DARK_GRAY);
                    FontMetrics fm = g2D.getFontMetrics();
                    g2D.drawString(df.format(c.getTime()), 0, fm.getAscent());
                    c.add(Calendar.MONTH, 1);
                }
                images.add(buffer);
            }

            if (createLegend) {
                int legendWidth = shape[2];
                int legendHeight = 60;
                if (legendWidth < 200)
                    legendWidth = 510;

                MutableImage myi = new MutableImage();
                for (int i = 0; i < images.size(); i++) {
                    myi.combineImagesVertical((BufferedImage) images.get(i), col.getLegendImage(legendWidth, legendHeight, 28, dataVar.getDescription(), dataVar.getUnitsString()));
                    images.set(i, myi.getImage());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        // grab the output image type from the first image in the sequence
        try {
            BufferedImage firstImage = (BufferedImage) images.get(0);

            // create a new BufferedOutputStream with the last argument
            ImageOutputStream output =
                    new FileImageOutputStream(new File(outputName + ".gif"));

            // create a gif sequence with the type of the first image, 1 second
            // between frames, which loops continuously
            GifSequenceWriter writer =
                    new GifSequenceWriter(output, firstImage.getType(), gifInterval, true);

            // write out the first image to our sequence...
            writer.writeToSequence(firstImage);
            for (int i = 1; i < images.size(); i++) {
                BufferedImage nextImage = (BufferedImage) images.get(i);
                writer.writeToSequence(nextImage);
            }
            writer.close();
            output.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * makeGif
     * creates a looped animated .gif of the images given in fileList, played at 1 image / second
     *
     * @param fileList   ArrayList of the file paths of the images to be combined
     * @param outputName name of the gif file
     */
    public void makeGifFromImages(ArrayList<String> fileList, String outputName) {
        if (outputName.toLowerCase().contains(".gif"))
            outputName = outputName.substring(0, outputName.length() - 4);

        if (fileList.size() > 1) {
            // grab the output image type from the first image in the sequence
            try {
                BufferedImage firstImage = ImageIO.read(new File(fileList.get(0)));

                // create a new BufferedOutputStream with the last argument
                ImageOutputStream output =
                        new FileImageOutputStream(new File(outputName + ".gif"));

                // create a gif sequence with the type of the first image, 1 second
                // between frames, which loops continuously
                GifSequenceWriter writer =
                        new GifSequenceWriter(output, firstImage.getType(), 1000, true);

                // write out the first image to our sequence...
                writer.writeToSequence(firstImage);
                for (int i = 1; i < fileList.size(); i++) {
                    BufferedImage nextImage = ImageIO.read(new File(fileList.get(i)));
                    writer.writeToSequence(nextImage);
                }
                writer.close();
                output.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        } else {
            System.out.println(
                    "Usage: java GifSequenceWriter [list of gif files] [output file]");
        }
    }

    /**
     * interpolateDataHdf
     * Creates a NetCDF file using a HDF file, copying a specified variable over with a data array that is interpolated based on idw interpolation
     *
     * @param fileName        HDF file to open
     * @param outputName      name of output file
     * @param var             variable short name
     * @param scale           new array dimensions of scale*given data dimension
     * @param thresholdFactor range that the IDW interpolation use (recommended: 2f)
     */
    public void interpolateDataHdf(String fileName, String outputName, String var, int scale, float thresholdFactor) {
        if (outputName.toLowerCase().contains(".nc"))
            outputName = outputName.substring(0, outputName.length() - 3);

        NetcdfFileWriteable outFile = new NetcdfFileWriteable();    //Create the file
        NetcdfFile myDataFile = null;                                //reference file

        try {
            outFile = NetcdfFileWriteable.createNew(outputName+".nc", false);
            myDataFile = NetcdfFile.open(fileName, null);


            Variable dataVar = myDataFile.findVariable(var);                //check if the variable is found

            if (dataVar == null)
                for (Variable v : myDataFile.getVariables()) {
                    if (v.getShortName().equals(var))
                        dataVar = v;
                }

            if (dataVar == null) {
                System.out.println("Cannot find Variable data " + var);
                return;
            }

            // add dimensions
           /* Dimension time = outFile.addDimension("TIME", 1);
            Dimension xDim = outFile.addDimension("XDim", 361);
            Dimension yDim = outFile.addDimension("YDim",540);


            // define Variable

            dims.add( time);
            dims.add( xDim);
            dims.add( yDim);*/
            ArrayList dims = new ArrayList();
            for (Dimension d : myDataFile.getDimensions()) {
                if (!d.getFullName().contains("_")) {
                    Dimension temp;
                    if (d.getFullName().contains("X") || d.getFullName().contains("Y"))
                        temp = outFile.addDimension(d.getFullName(), d.getLength() * scale);

                    else
                        temp = outFile.addDimension(d.getFullName(), d.getLength());

                    //noinspection unchecked
                    dims.add(temp);
                    System.out.println(d.getFullName());
                }
            }
            outFile.addVariable(var, dataVar.getDataType(), dims);


            for (Attribute a : dataVar.getAttributes()) {
                if (a.getDataType() == DataType.STRING)
                    outFile.addVariableAttribute(var, a.getFullName(), a.getStringValue());
                else
                    outFile.addVariableAttribute(var, a.getFullName(), a.getNumericValue());
            }


            // add global attributes
            for (Attribute a : myDataFile.getGlobalAttributes()) {
                outFile.addGlobalAttribute(a);
            }
            Attribute missingValue = myDataFile.findGlobalAttribute("missing_value");
            D3 idw;
            if (missingValue == null) {
                idw = interpolateArray((D3) dataVar.read(), scale, thresholdFactor);
            } else {
                idw = interpolateArray((D3) dataVar.read(), scale, thresholdFactor, missingValue.getNumericValue().floatValue());
            }
            outFile.create();
            outFile.write(var, idw);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != outFile)
                try {
                    outFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }

            if (null != myDataFile)
                try {
                    myDataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        }
    }

    /**
     * interpolateData
     * This function creates a new NetCDF file that contains one interpolated data set for the specified variable using one given NetCDF file
     *
     * @param fileName        name of NetCDF file
     * @param outputName      name of output file
     * @param var             variable short name
     * @param scale           array resolution is time x lat*scale x long*scale
     * @param thresholdFactor range that the IDW interpolation use (recommended: 2f)
     */
    public void interpolateData(String fileName, String outputName, String var, int scale, float thresholdFactor) {
        if (outputName.toLowerCase().contains(".nc"))
            outputName = outputName.substring(0, outputName.length() - 3);

        NetcdfFileWriteable outFile = new NetcdfFileWriteable();    //Create the file
        NetcdfFile myDataFile = null;                                //reference file

        try {
            outFile = NetcdfFileWriteable.createNew(outputName+".nc", false);
            myDataFile = NetcdfFile.open(fileName, null);


            Variable dataVar = myDataFile.findVariable(var);                //check if the variable is found

            if (dataVar == null)
                for (Variable v : myDataFile.getVariables()) {
                    if (v.getShortName().equals(var))
                        dataVar = v;
                }

            if (dataVar == null) {
                System.out.println("Cannot find Variable data " + var);
                return;
            }

            //Define DIMENSIONS
            List<Dimension> myListD = new ArrayList<Dimension>();
            for (Dimension d : myDataFile.getDimensions()) {
                Dimension temp;
                if ((d.getName().contains("latitude") || d.getName().contains("longitude")) || ((d.getName().contains("YDim") || (d.getName().contains("XDim")))))      //look for the latitude and longitude dimensions and adjust the resolution
                    temp = new Dimension(d.getName(), d.getLength() * scale);

                else if (d.getName().equals("time")) {
                    temp = new Dimension(d.getName(), d.getLength());               //make time limited to agree with the finite dimensions being used in calculations
                    temp.setUnlimited(false);
                    d.setUnlimited(false);
                } else {
                    temp = new Dimension(d.getName(), d.getLength());
                }
                myListD.add(temp);
            }

            for (Dimension d : myListD)                                //add the dimensions to the new file
            {
                outFile.addDimension(d.getFullName(), d.getLength(), d.isShared(), d.isUnlimited(), d.isVariableLength());
            }

            //Define GLOBAL ATTRIBUTES
            List<Attribute> AL = myDataFile.getGlobalAttributes();    //first, model all the global attributes from the file. Then, append the values for each attribute, separated by "; "
            for (int j = 0; j < AL.size(); j++)                    //iterate through each attribute for the AL and tempAL
            {
                Attribute a = AL.get(j);
                if (a.getDataType() == DataType.STRING)            //look to append attributes that hold strings
                {
                    AL.set(j, new Attribute(a.getFullName(), a.getStringValue()));
                } else {
                    AL.set(j, new Attribute(a.getFullName(), a.getNumericValue().toString()));
                }
            }

            for (Attribute a : AL)                                    //add the new Attribute List to the output file
            {
                outFile.addGlobalAttribute(a);
            }


            //interpolate each lat x long 2D array of data throughout the time dimension
            //this assumes the array has the dimensions time x lat x long


            Attribute missingValue = myDataFile.findGlobalAttribute("missing_value");   //see if there is a global attribute for the missing values
            D3 idw;
            if (missingValue == null) {   //continue normally
                idw = interpolateArray((D3) dataVar.read(), scale, thresholdFactor);
            } else {
                idw = interpolateArray((D3) dataVar.read(), scale, thresholdFactor, missingValue.getNumericValue().floatValue());
            }

            List<Dimension> ld = dataVar.getDimensions();
            ld.get(1).setLength(ld.get(1).getLength() * scale);     //adjust the lat and long dimensions
            ld.get(2).setLength(ld.get(2).getLength() * scale);

            outFile.addVariable(var, dataVar.getDataType(), ld);        //add the new variable


            for (Attribute s : dataVar.getAttributes())                                        //add in the attributes for the variable
            {
                if (s.getDataType() == DataType.STRING)
                    outFile.addVariableAttribute(var, s.getShortName(), s.getStringValue());

                else
                    outFile.addVariableAttribute(var, s.getShortName(), s.getNumericValue());


            }

            // create the file; definition phase is over, writing phase begins
            outFile.create();
            //write in the data
            int[] origin = new int[3];
            outFile.write(var, origin, idw);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != outFile)
                try {
                    outFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }

            if (null != myDataFile)
                try {
                    myDataFile.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        }
    }
}
