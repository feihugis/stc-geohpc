package edu.gmu.stc.datavisualization.netcdf;


import ucar.ma2.ArrayFloat;

import javax.imageio.ImageIO;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.jfree.data.time.TimeSeriesCollection;

import java.awt.*;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;


public class PngWriter {
    static Comparator<Float> myComp = new Comparator<Float>() {
        public int compare(Float a, Float b) {
            return (a.compareTo(b));
        }
    };

    static Comparator<Double> myCompD = new Comparator<Double>() {
        public int compare(Double a, Double b) {
            return a.compareTo(b);
        }
    };
    private static int[][] rgb;
    private static Font mFont = new Font("Tahoma", Font.PLAIN, 13);
    private File outFile = null;
    private File fi = null;
    private BufferedImage bi = null;
    private File fo = null;
    private BufferedImage image = null;
    private int width, height;

    public PngWriter() {

    }

    public PngWriter(String inputImage) {
        fi = new File(inputImage);
        try {
            System.out.println(fi.getAbsolutePath());
            Image inImage = ImageIO.read(fi);
            width = inImage.getWidth(null);
            height = inImage.getHeight(null);
            BufferedImage imIn = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);

            imIn.getGraphics().drawImage(inImage, 0, 0, width, height, null);
            rgb = new int[width][height];

            for (int i = 0; i < width; i++) {
                for (int j = 0; j < height; j++) {


                    rgb[i][j] = imIn.getRGB(i, j);

                    //System.out.println("rgb:"+rgb[i][j]);
                }
            }
            outFile = new File("sdg.png");

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public PngWriter(String outputName, MutableImage inputImage) {
        image = inputImage.getImage();
        outFile = new File(outputName + ".png");
    }


    
    public PngWriter(String outputName, int w, int h) {
        width = w;
        height = h;
        outFile = new File(outputName + ".png");
    }

    public PngWriter(String outputName, int[][] rgbb, int w, int h) {
        width = w;
        height = h;
        outFile = new File(outputName + ".png");
        rgb = rgbb;
    }

    public static float findMaxVal(ArrayFloat.D1 ar) {

        float max = ar.getFloat(0);

        for (float f : ar.getShape()) {
            if (f > max)
                max = f;

        }
        return max;

    }

    public static float findMaxVal(ArrayFloat.D2 ar) {
        int[] shape = ar.getShape();
        float max = ar.get(0, 0);
        for (int i = 0; i < shape[0]; i++) {
            for (int j = 0; j < shape[1]; j++) {
                if (max < ar.get(i, j))
                    max = ar.get(i, j);
            }
        }
        //System.out.println(max);
        return max;

    }

    public static float findMinVal(ArrayFloat.D2 ar) {
        int[] shape = ar.getShape();
        float min = ar.get(0, 0);
        for (int i = 0; i < shape[0]; i++) {
            for (int j = 0; j < shape[1]; j++) {
                if (min > ar.get(i, j))
                    min = ar.get(i, j);
            }
        }
        return min;

    }

    public void createImage() {
        bi = createBuffer(rgb, outFile);
    }

    public void createMutableImage() {
        bi = image;
        createBuffer(image, outFile);
    }

    public void createImage(int[][] irgb, File fo) {
        //write rgb to any location
        bi = createBuffer(irgb, fo);
    }

    public void createImage(int[][] irgb, File fo, String[] legend, String para, String units) {
        //write rgb to any location
        bi = createBuffer(irgb, fo, legend, para, units);
    }

    public void createImage(int[][] irgb, File fo, String[] legend) {
        //write rgb to any location
        bi = createBuffer(irgb, fo, legend);
    }

    public void createImage(File fo) {
        int irgb[][] = new int[32][32];
        for (int i = 0; i < irgb.length; i++) {
            for (int j = 0; j < irgb[i].length; j++) {
                irgb[i][j] = 0;
            }
        }
        bi = createBuffer(irgb, fo);

    }

    private void createBuffer(BufferedImage im, File f) {
        String format = "PNG";

        try {
            ImageIO.write(im, format, f);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private BufferedImage createBuffer(int[][] irgb, File f) {
        //System.out.println(f.getAbsolutePath());

        BufferedImage buffer = new BufferedImage(irgb.length, irgb[0].length, BufferedImage.TYPE_INT_ARGB);

        Graphics2D g2D = buffer.createGraphics();
        g2D.setComposite(AlphaComposite.getInstance(AlphaComposite.CLEAR, 0.0f));
        Rectangle2D.Double rect = new Rectangle2D.Double(0, 0, irgb.length, irgb[0].length);
        g2D.fill(rect);

        for (int i = 0; i < irgb.length; i++) {
            for (int j = 0; j < irgb[i].length; j++) {
                if (irgb[i][j] != -1) {
                    buffer.setRGB(i, j, irgb[i][j]);
                    //System.out.println(irgb[i][j]);
                }
            }
        }
        String format = "PNG";

        try {
            ImageIO.write(buffer, format, f);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return buffer;
    }

    private BufferedImage createBuffer(int[][] irgb, File f, String[] legend) {

        BufferedImage buffer = new BufferedImage(irgb.length, irgb[0].length, BufferedImage.TYPE_INT_ARGB);

		/*Graphics2D g2D =  buffer.createGraphics();
		g2D.setComposite(AlphaComposite.getInstance(AlphaComposite.CLEAR, 0.0f));
		Rectangle2D.Double rect = new Rectangle2D.Double(0,0,irgb.length,irgb[0].length);
		g2D.draw(rect);
		g2D.fill(rect);
		g2D.setColor(Color.black);
		g2D.setFont(mFont);
		g2D.drawString("**********", 0,0);

		*/

        //BufferedImage buffer = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics g = buffer.getGraphics();
//        g.setColor(getRandColor(200,250));
        // g.fillRect(1, 1, width-1, height-1);
        g.setColor(Color.black);
        // g.drawRect(0, 0, width-1, height-1);
        g.setFont(mFont);
        for (int k = 0; k < legend.length; k++) {
            g.drawString(legend[k], 10 + 75 * k, irgb[0].length / 2 + 15);
        }


        for (int i = 0; i < irgb.length; i++) {
            for (int j = 0; j < irgb[i].length / 2; j++) {
                if (irgb[i][j] != -1) {
                    buffer.setRGB(i, j, irgb[i][j]);

                }
            }
        }


        String format = "PNG";

        try {
            ImageIO.write(buffer, format, f);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return buffer;
    }

    private BufferedImage createBuffer(int[][] irgb, File f, String[] legend, String para, String units) {

        BufferedImage buffer = new BufferedImage(irgb.length, irgb[0].length, BufferedImage.TYPE_INT_ARGB);

		/*Graphics2D g2D =  buffer.createGraphics();
		g2D.setComposite(AlphaComposite.getInstance(AlphaComposite.CLEAR, 0.0f));
		Rectangle2D.Double rect = new Rectangle2D.Double(0,0,irgb.length,irgb[0].length);
		g2D.draw(rect);
		g2D.fill(rect);
		g2D.setColor(Color.black);
		g2D.setFont(mFont);
		g2D.drawString("**********", 0,0);

		*/

        //BufferedImage buffer = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics g = buffer.getGraphics();
//        g.setColor(getRandColor(200,250));
        // g.fillRect(1, 1, width-1, height-1);
        g.setColor(Color.red);
        // g.drawRect(0, 0, width-1, height-1);
        g.setFont(mFont);
        //centralized the fonts
        FontMetrics FM = g.getFontMetrics();
        int Ascent = FM.getAscent();
        int Descent = FM.getDescent();
        int Width = FM.stringWidth(para);
        int wCenter = (irgb.length - Width) / 2;


        g.drawString(para + " (" + units + ")", wCenter, 15);        //title
        int spacing = (irgb.length - 20) / legend.length;
        for (int k = 0; k < legend.length; k++) {
            g.drawString(legend[k], 10 + spacing * k, 2 * irgb[0].length / 3 + 15);
        }


        for (int i = 0; i < irgb.length; i++) {
            for (int j = 20; j < 2 * irgb[i].length / 3; j++) {
                if (irgb[i][j] != -1) {
                    buffer.setRGB(i, j, irgb[i][j]);

                }
            }
        }


        String format = "PNG";

        try {
            ImageIO.write(buffer, format, f);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return buffer;
    }

    public BufferedImage getBuffer(int[][] irgb, File f, String[] legend, int[] marks, String para, String units, int margin) {

        BufferedImage buffer = new BufferedImage(irgb.length, irgb[0].length, BufferedImage.TYPE_INT_ARGB);

		/*Graphics2D g2D =  buffer.createGraphics();
		g2D.setComposite(AlphaComposite.getInstance(AlphaComposite.CLEAR, 0.0f));
		Rectangle2D.Double rect = new Rectangle2D.Double(0,0,irgb.length,irgb[0].length);
		g2D.draw(rect);
		g2D.fill(rect);
		g2D.setColor(Color.black);
		g2D.setFont(mFont);
		g2D.drawString("**********", 0,0);

		*/

        //BufferedImage buffer = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics g = buffer.getGraphics();
//        g.setColor(getRandColor(200,250));
        // g.fillRect(1, 1, width-1, height-1);
        g.setColor(Color.red);
        // g.drawRect(0, 0, width-1, height-1);
        g.setFont(mFont);
        //centralized the fonts
        FontMetrics FM = g.getFontMetrics();
        int Ascent = FM.getAscent();
        int Descent = FM.getDescent();
        int Width = FM.stringWidth(para);
        int wCenter = (irgb.length - Width) / 2;


        g.drawString(para + " (" + units + ")", wCenter, 15);        //title

        for (int k = 0; k < legend.length; k++) {
            g.drawString(legend[k], marks[k] - margin / 2, 2 * irgb[0].length / 3 + 15);
        }


        for (int i = 0; i < irgb.length; i++) {
            for (int j = 20; j < 2 * irgb[i].length / 3; j++) {
                if (irgb[i][j] != -1) {
                    buffer.setRGB(i, j, irgb[i][j]);

                }
            }
        }


        String format = "PNG";

        return buffer;
    }

    public BufferedImage getBuffer(int[][] irgb, File f, String[] legend, String para, String units, int margin) {

        BufferedImage buffer = new BufferedImage(irgb.length, irgb[0].length, BufferedImage.TYPE_INT_ARGB);

		/*Graphics2D g2D =  buffer.createGraphics();
		g2D.setComposite(AlphaComposite.getInstance(AlphaComposite.CLEAR, 0.0f));
		Rectangle2D.Double rect = new Rectangle2D.Double(0,0,irgb.length,irgb[0].length);
		g2D.draw(rect);
		g2D.fill(rect);
		g2D.setColor(Color.black);
		g2D.setFont(mFont);
		g2D.drawString("**********", 0,0);

		*/

        //BufferedImage buffer = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics g = buffer.getGraphics();
//        g.setColor(getRandColor(200,250));
        // g.fillRect(1, 1, width-1, height-1);
        g.setColor(Color.red);
        // g.drawRect(0, 0, width-1, height-1);
        g.setFont(mFont);
        //centralized the fonts
        FontMetrics FM = g.getFontMetrics();
        int Ascent = FM.getAscent();
        int Descent = FM.getDescent();
        int Width = FM.stringWidth(para);
        int wCenter = (irgb.length - Width) / 2;


        g.drawString(para + " (" + units + ")", wCenter, 15);        //title
        int spacing = (irgb.length) / legend.length;
        for (int k = 0; k < legend.length; k++) {
            g.drawString(legend[k], margin + spacing * k, 2 * irgb[0].length / 3 + 15);
        }


        for (int i = 0; i < irgb.length; i++) {
            for (int j = 20; j < 2 * irgb[i].length / 3; j++) {
                if (irgb[i][j] != -1) {
                    buffer.setRGB(i, j, irgb[i][j]);

                }
            }
        }


        String format = "PNG";

        return buffer;
    }

    public BufferedImage getInterpolateImageBilinear(String outputName, int ratio) {
        BufferedImage buffer = new BufferedImage(rgb.length, rgb[0].length, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2D = buffer.createGraphics();
        g2D.setComposite(AlphaComposite.getInstance(AlphaComposite.CLEAR, 0.0f));
        Rectangle2D.Double rect = new Rectangle2D.Double(0, 0, rgb.length, rgb[0].length);
        g2D.fill(rect);

        for (int i = 0; i < rgb.length; i++) {
            for (int j = 0; j < rgb[i].length; j++) {
                if (rgb[i][j] != -1) {
                    buffer.setRGB(i, j, rgb[i][j]);
                    //System.out.println(irgb[i][j]);
                }
            }
        }
        try {
            int w = buffer.getWidth(null);
            int h = buffer.getHeight(null);

            BufferedImage bilinear = new BufferedImage(ratio * w, ratio * h,
                    BufferedImage.TYPE_INT_ARGB);


            Graphics2D bg = bilinear.createGraphics();
            bg.setRenderingHint(RenderingHints.KEY_INTERPOLATION,
                    RenderingHints.VALUE_INTERPOLATION_BILINEAR);


            bg.scale(ratio, ratio);
            bg.drawImage(buffer, 0, 0, null);
            bg.dispose();


            File f = new File(outputName + ".png");


            String format = "PNG";


        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return buffer;
    }

    public void interpolateImageBilinear(String outputName, int ratio) {
        BufferedImage buffer = new BufferedImage(rgb.length, rgb[0].length, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2D = buffer.createGraphics();
        g2D.setComposite(AlphaComposite.getInstance(AlphaComposite.CLEAR, 0.0f));
        Rectangle2D.Double rect = new Rectangle2D.Double(0, 0, rgb.length, rgb[0].length);
        g2D.fill(rect);

        for (int i = 0; i < rgb.length; i++) {
            for (int j = 0; j < rgb[i].length; j++) {
                if (rgb[i][j] != -1) {
                    buffer.setRGB(i, j, rgb[i][j]);
                    //System.out.println(irgb[i][j]);
                }
            }
        }
        try {
            int w = buffer.getWidth(null);
            int h = buffer.getHeight(null);

            BufferedImage bilinear = new BufferedImage(ratio * w, ratio * h,
                    BufferedImage.TYPE_INT_ARGB);


            Graphics2D bg = bilinear.createGraphics();
            bg.setRenderingHint(RenderingHints.KEY_INTERPOLATION,
                    RenderingHints.VALUE_INTERPOLATION_BILINEAR);


            bg.scale(ratio, ratio);
            bg.drawImage(buffer, 0, 0, null);
            bg.dispose();


            File f = new File(outputName + ".png");


            String format = "PNG";

            try {
                ImageIO.write(bilinear, format, f);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void interpolateImageBicubic(String outputName, int ratio) {
        BufferedImage buffer = new BufferedImage(rgb.length, rgb[0].length, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2D = buffer.createGraphics();
        g2D.setComposite(AlphaComposite.getInstance(AlphaComposite.CLEAR, 0.0f));
        Rectangle2D.Double rect = new Rectangle2D.Double(0, 0, rgb.length, rgb[0].length);
        g2D.fill(rect);

        for (int i = 0; i < rgb.length; i++) {
            for (int j = 0; j < rgb[i].length; j++) {
                if (rgb[i][j] != -1) {
                    buffer.setRGB(i, j, rgb[i][j]);
                    //System.out.println(irgb[i][j]);
                }
            }
        }
        try {
            int w = buffer.getWidth(null);
            int h = buffer.getHeight(null);

            BufferedImage bilinear = new BufferedImage(ratio * w, ratio * h,
                    BufferedImage.TYPE_INT_ARGB);


            Graphics2D bg = bilinear.createGraphics();
            bg.setRenderingHint(RenderingHints.KEY_INTERPOLATION,
                    RenderingHints.VALUE_INTERPOLATION_BICUBIC);


            bg.scale(ratio, ratio);
            bg.drawImage(buffer, 0, 0, null);
            bg.dispose();


            File f = new File(outputName + ".png");


            String format = "PNG";

            try {
                ImageIO.write(bilinear, format, f);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void interpolateImageNearestNeighbor(String outputName, int ratio) {
        BufferedImage buffer = new BufferedImage(rgb.length, rgb[0].length, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2D = buffer.createGraphics();
        g2D.setComposite(AlphaComposite.getInstance(AlphaComposite.CLEAR, 0.0f));
        Rectangle2D.Double rect = new Rectangle2D.Double(0, 0, rgb.length, rgb[0].length);
        g2D.fill(rect);

        for (int i = 0; i < rgb.length; i++) {
            for (int j = 0; j < rgb[i].length; j++) {
                if (rgb[i][j] != -1) {
                    buffer.setRGB(i, j, rgb[i][j]);
                    //System.out.println(irgb[i][j]);
                }
            }
        }
        try {
            int w = buffer.getWidth(null);
            int h = buffer.getHeight(null);

            BufferedImage bilinear = new BufferedImage(ratio * w, ratio * h,
                    BufferedImage.TYPE_INT_ARGB);


            Graphics2D bg = bilinear.createGraphics();
            bg.setRenderingHint(RenderingHints.KEY_INTERPOLATION,
                    RenderingHints.VALUE_INTERPOLATION_NEAREST_NEIGHBOR);


            bg.scale(ratio, ratio);
            bg.drawImage(buffer, 0, 0, null);
            bg.dispose();


            File f = new File(outputName + ".png");


            String format = "PNG";

            try {
                ImageIO.write(bilinear, format, f);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    //ALTER AVG TO WRITE ONLY ONE VARIABLE


    //LEGEND MAKER FUNCTION
    //legend to accomodate many things over time 12 months same legend


    // NETCDF FUNCTION
    //function to store interpolated data into a new net cdf  ( 2 types)


    // .gif MAKER FUNCTION
    // .gif maker Inputs: arraylist of file names, output name
    // outputs: .gif


    // PNG MAKER FUNCTION
    // inputs
    //file name
    //check time dimension
    // variable
    // output type
    // scale
    // interpolation

    //outputs multiple .png or animated .gif


}
