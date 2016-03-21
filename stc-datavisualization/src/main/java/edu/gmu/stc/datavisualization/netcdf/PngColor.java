package edu.gmu.stc.datavisualization.netcdf;


import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.text.DecimalFormat;

public class PngColor {

    private int[] rgb = new int[8];
    private float max;
    private float min;
    //= Color.HSBtoRGB(0f, 0.938f, 90);
    //Color nRGB = new Color((rgb));
    private int[][] legendMap;
    private String[] lengNum = new String[7];

    public PngColor() {

    }

    public PngColor(float inMin, float inMax) {
        this.max = inMax;
        this.min = inMin;
        legendMap = new int[510][60];
    }

    //input: a series of data
    //convert the attribute value into color
    public String[] getLegend() {
        return lengNum;
    }
    /*
	public PngColor(float inMin, float inMax, String para,String units)
    {
        this.max = inMax;
        this.min = inMin;
        this.variable = para;
        this.units=units;
        legendMap = new int[510][60];
    }

    public PngColor(float inMin, float inMax, String para, String units, int width, int height)
    {
        this.max = inMax;
        this.min = inMin;
        this.variable = para;
        this.units=units;
        legendMap = new int[width][height];
    }*/

    public void trans(double left, double right, double up, double down) {
        double k1 = (44 - 24.9) / (42.68 - 29.53);
        double k2 = (-98.7 - (-144.6)) / (-78 - (-125));
        double newUp = up - 1.4;
        double newDown = newUp - (up - down) / k1;
        double newRight = ((right - left) / 2 + left) + k2 * (right - left) / 4;
        double newLeft = ((right - left) / 2 + left) - k2 * (right - left) / 4;
    }


    //convert any data to color
    public int getColorRGB(float data) {
        //int color1=160;//(int) ((data-this.min)/(max-min)*8);
        //System.out.println(color1);
        //normalized:0-1
        float normal = (data - this.min) / (max - min);
        int rgb = Color.HSBtoRGB((1f - normal) / 1.5f, 0.94f, 0.5f + normal * 0.33f);

        /*if (data < min/2) {
          rgb = 0xff000000 | (255 << 16) | (255 << 8) | (255 << 0);
        }*/

        return rgb;
    }
	
	/*public void createLegend(String location)
	{
		for(int i=0 ; i <legendMap.length; i++)
		{
			for(int j=0; j< legendMap[i].length; j++)
			{
				if(i<10||i>410)
					legendMap[i][j]=-1;
				else{
				float normal =(float) ((i-10)*1.0/(legendMap.length-20));
				legendMap[i][j]=  Color.HSBtoRGB((1f-normal)/1.5f, 0.94f, 0.5f+normal*0.33f);
				}
			}
		}
		
		File legend = new File(location);
		PngWriter pg = new PngWriter();
		String[] lengNum = new String[6];
		for(int k = 0 ; k <lengNum.length; k++)
		{
			float inLegend =  this.min+(this.max-this.min)/11f*k;
			DecimalFormat myformat=new DecimalFormat("0.000"); 
			lengNum[k] = (myformat.format(inLegend));
		}
		if(this.para!=null)
		pg.createImage(this.legendMap, legend,lengNum,para);
		else
			pg.createImage(this.legendMap, legend,lengNum);
	}*/

    /**
     * createLegend
     * creates a legend .png using the min, max, legendMap, variable name, and units globals
     *
     * @param outputName
     */
    public void createLegend(String outputName) {
        for (int i = 0; i < legendMap.length; i++) {
            for (int j = 0; j < legendMap[i].length; j++) {
                if (i < 10 || i > 500)
                    legendMap[i][j] = -1;
                else {
                    float normal = (float) ((i - 10) * 1.0 / (legendMap.length - 20));
                    legendMap[i][j] = Color.HSBtoRGB((1f - normal) / 1.5f, 0.94f, 0.5f + normal * 0.33f);
                }
            }
        }

        File legend = new File(outputName + ".png");
        PngWriter pg = new PngWriter();
        String[] lengNum = new String[7];
        //System.out.println(""+this.max+" "+this.min+" "+(this.max-this.min));
        for (int k = 0; k < lengNum.length; k++) {
            float inLegend = this.min + (this.max - this.min) / 6f * k;
            DecimalFormat myformat = null;
            if (Math.abs(inLegend) > 100000)
                myformat = new DecimalFormat("0.00E0");    //correcting the format of excessively large numbers

            else
                myformat = new DecimalFormat("0.000");

            lengNum[k] = (myformat.format(inLegend));
        }
        pg.createImage(this.legendMap, legend, lengNum);
    }

    public void createLegend(String outputName, int width, int height, int margin, String title, String units) {
        legendMap = new int[width][height];
        for (int i = 0; i < legendMap.length; i++) {
            for (int j = 0; j < legendMap[i].length; j++) {
                if (i < margin || i > width - margin)
                    legendMap[i][j] = -1;
                else {
                    float normal = (float) ((i - margin) * 1.0 / (legendMap.length - 2 * margin));
                    legendMap[i][j] = Color.HSBtoRGB((1f - normal) / 1.5f, 0.94f, 0.5f + normal * 0.33f);
                }
            }
        }

        File legend = new File(outputName + ".png");
        PngWriter pg = new PngWriter();
        String[] lengNum = new String[5];
        //System.out.println(""+this.max+" "+this.min+" "+(this.max-this.min));
        for (int k = 0; k < lengNum.length; k++) {
            float inLegend = this.min + (this.max - this.min) / 4f * k;
            DecimalFormat myformat = null;
            if (Math.abs(inLegend) > 100000)
                myformat = new DecimalFormat("0.0E0");    //correcting the format of excessively large numbers

            else
                myformat = new DecimalFormat("0.0");

            lengNum[k] = (myformat.format(inLegend));
        }
        pg.createImage(this.legendMap, legend, lengNum, title, units);
    }


    public BufferedImage getLegendImage(int width, int height, int margin, String title, String units) {
        legendMap = new int[width][height];
        int[] marks = new int[width / 50];
        double spacing = (((double) (width - margin * 2)) / ((double) marks.length - 1.0));
        for (int i = 0; i < marks.length; i++) {
            marks[i] = margin + (int) Math.round(spacing * i);
        }
        boolean skip = false;
        for (int i = 0; i < legendMap.length; i++) {
            for (int j = 0; j < legendMap[i].length; j++) {
                skip = false;
                for (int a : marks) {
                    if (a == i && j > 30 && j < 40) {
                        legendMap[i][j] = -16777216;
                        skip = true;
                    }
                }
                if (!skip) {
                    if (i < margin || i > legendMap.length - margin)
                        legendMap[i][j] = -1;

                    else {
                        float normal = (float) ((i - margin) * 1.0 / (legendMap.length - 2 * margin));
                        legendMap[i][j] = Color.HSBtoRGB((1f - normal) / 1.5f, 0.94f, 0.5f + normal * 0.33f);
                    }
                }
            }
        }

        File legend = new File("hellog.png");
        PngWriter pg = new PngWriter();
        String[] lengNum = new String[marks.length];
        //System.out.println(""+this.max+" "+this.min+" "+(this.max-this.min));
        for (int k = 0; k < lengNum.length; k++) {
            float inLegend = this.min + (this.max - this.min) / ((float) (lengNum.length - 1)) * k;
            DecimalFormat myformat = null;
            if (Math.abs(inLegend) > 100000)
                myformat = new DecimalFormat("0.0E0");    //correcting the format of excessively large numbers

            else
                myformat = new DecimalFormat("0.0");

            lengNum[k] = (myformat.format(inLegend));
        }
        return pg.getBuffer(this.legendMap, legend, lengNum, marks, title, units, margin);
    }

}
