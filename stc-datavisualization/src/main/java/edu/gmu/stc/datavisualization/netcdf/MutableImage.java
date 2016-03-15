package edu.gmu.stc.datavisualization.netcdf;

import sun.awt.image.BufferedImageGraphicsConfig;

import java.awt.*;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.awt.image.BufferedImageOp;
import java.awt.image.ConvolveOp;
import java.awt.image.Kernel;
import java.util.HashMap;
import java.util.Map;

/**
 * MutableImage.java
 * Object for operations on a BufferedImage
 *
 * Created by Matt on 7/30/2014.
 */
public class MutableImage {
    private BufferedImage image;
    private int width;
    private int height;

    public MutableImage() {

    }

    public MutableImage(int w, int h) {
        width = w;
        height = h;
        image = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);
    }

    public MutableImage(BufferedImage i) {
        width = i.getWidth();
        height = i.getHeight();
        image = i;
    }

    public MutableImage(int[][] rgb, int w, int h) {
        width = w;
        height = h;
        image = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2D = image.createGraphics();
        g2D.setComposite(AlphaComposite.getInstance(AlphaComposite.CLEAR, 0.0f));
        Rectangle2D.Double rect = new Rectangle2D.Double(0, 0, rgb.length, rgb[0].length);
        g2D.fill(rect);

        for (int y = 0; y < rgb.length; y++) {
            for (int x = 0; x < rgb[y].length; x++) {
                if (rgb[y][x] != -1) {
                    image.setRGB(x, y, rgb[y][x]);
                }
            }
        }
    }

    /**
     * combineImagesHorizontal
     * appends two buffered images, first parameter on left
     *
     * @param i1    first image, appears on left
     * @param i2    second image, appears on right
     */
    public void combineImagesHorizontal(BufferedImage i1, BufferedImage i2) {
        width = i1.getWidth() + i2.getWidth();
        height = maxHeight(i1, i2);
        image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);

        Graphics2D g = (Graphics2D) image.getGraphics();
        g.drawImage(i1, 0, 0, null);
        g.drawImage(i2, i1.getWidth(), 0, null);
        g.dispose();
    }

    /**
     * combineImagesVertical
     * appends two buffered images, first parameter on top
     *
     * @param i1    first image, appears on top
     * @param i2    second image, appears on bottom
     */
    public void combineImagesVertical(BufferedImage i1, BufferedImage i2) {
        width = maxWidth(i1, i2);
        height = i1.getHeight() + i2.getHeight();
        image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);

        Graphics2D g = (Graphics2D) image.getGraphics();
        g.drawImage(i1, 0, 0, null);
        g.drawImage(i2, 0, i1.getHeight(), null);
        g.dispose();
    }

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }

    public BufferedImage getImage() {
        return image;
    }

    public void setImage(BufferedImage i) {
        width = i.getWidth();
        height = i.getHeight();
        image = i;
    }

    public void drawString(String text, int x, int y, Font font, Color color) {
        Graphics2D g = (Graphics2D) image.getGraphics();
        g.setComposite(AlphaComposite.Src);
        g.setRenderingHint(
                RenderingHints.KEY_ANTIALIASING,
                RenderingHints.VALUE_ANTIALIAS_ON);
        g.setFont(font);
        g.setColor(color);
        FontMetrics fm = g.getFontMetrics();
        g.drawString(text, x, y + fm.getAscent());
        g.dispose();
    }

    public int maxWidth(BufferedImage i1, BufferedImage i2) {
        if (i1.getWidth() > i2.getWidth())
            return i1.getWidth();

        else
            return i2.getWidth();
    }

    public int maxHeight(BufferedImage i1, BufferedImage i2) {
        if (i1.getHeight() > i2.getHeight())
            return i1.getHeight();

        else
            return i2.getHeight();
    }

    public BufferedImage createCompatibleImage(BufferedImage image) {
        GraphicsConfiguration gc = BufferedImageGraphicsConfig.getConfig(image);
        int w = image.getWidth();
        int h = image.getHeight();
        BufferedImage result = gc.createCompatibleImage(w, h, Transparency.TRANSLUCENT);
        Graphics2D g2 = result.createGraphics();
        g2.drawRenderedImage(image, null);
        g2.dispose();
        return result;
    }

    public BufferedImage resize(BufferedImage image, int width, int height) {
        BufferedImage resizedImage = new BufferedImage(width, height,
                BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = resizedImage.createGraphics();
        g.drawImage(image, 0, 0, width, height, null);
        g.dispose();
        return resizedImage;
    }

    public BufferedImage resizeBilinear(BufferedImage image, int width, int height) {
        BufferedImage resizedImage = new BufferedImage(width, height,
                BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = resizedImage.createGraphics();
        g.setRenderingHint(RenderingHints.KEY_INTERPOLATION,
                RenderingHints.VALUE_INTERPOLATION_BILINEAR);

        g.setRenderingHint(RenderingHints.KEY_RENDERING,
                RenderingHints.VALUE_RENDER_QUALITY);

        g.setRenderingHint(
                RenderingHints.KEY_ANTIALIASING,
                RenderingHints.VALUE_ANTIALIAS_ON);
        g.drawImage(image, 0, 0, width, height, null);
        g.dispose();
        return resizedImage;
    }


    public BufferedImage resizeBicubic(BufferedImage image, int width, int height) {
        BufferedImage resizedImage = new BufferedImage(width, height,
                BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = resizedImage.createGraphics();
        g.setRenderingHint(RenderingHints.KEY_INTERPOLATION,
                RenderingHints.VALUE_INTERPOLATION_BICUBIC);

        g.setRenderingHint(RenderingHints.KEY_RENDERING,
                RenderingHints.VALUE_RENDER_QUALITY);

        g.setRenderingHint(
                RenderingHints.KEY_ANTIALIASING,
                RenderingHints.VALUE_ANTIALIAS_ON);
        g.drawImage(image, 0, 0, width, height, null);
        g.dispose();
        return resizedImage;
    }

    public BufferedImage resizeNearestNeighbor(BufferedImage image, int width, int height) {
        BufferedImage resizedImage = new BufferedImage(width, height,
                BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = resizedImage.createGraphics();
        g.setRenderingHint(RenderingHints.KEY_INTERPOLATION,
                RenderingHints.VALUE_INTERPOLATION_NEAREST_NEIGHBOR);

        g.setRenderingHint(RenderingHints.KEY_RENDERING,
                RenderingHints.VALUE_RENDER_QUALITY);

        g.setRenderingHint(
                RenderingHints.KEY_ANTIALIASING,
                RenderingHints.VALUE_ANTIALIAS_ON);
        g.drawImage(image, 0, 0, width, height, null);
        g.dispose();
        return resizedImage;
    }

    public BufferedImage blurImage(BufferedImage image) {
        float ninth = 1.0f / 9.0f;
        float[] blurKernel = {
                ninth, ninth, ninth,
                ninth, ninth, ninth,
                ninth, ninth, ninth
        };

        Map map = new HashMap();

        //noinspection unchecked
        map.put(RenderingHints.KEY_INTERPOLATION,
                RenderingHints.VALUE_INTERPOLATION_BILINEAR);

        map.put(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
        map.put(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

        RenderingHints hints = new RenderingHints(map);
        BufferedImageOp op = new ConvolveOp(new Kernel(3, 3, blurKernel), ConvolveOp.EDGE_NO_OP, hints);
        return op.filter(image, null);
    }

}
