package edu.gmu.stc.datavisualization.netcdf;

public class FloatCoordinate {
    private float x;
    private float y;
    private float value;
    private boolean empty;

    public FloatCoordinate() {
        empty = true;
    }

    public FloatCoordinate(float xa, float ya) {
        x = xa;
        y = ya;
        value = 0;
        empty = false;
    }

    public FloatCoordinate(float xa, float ya, float val) {
        x = xa;
        y = ya;
        value = val;
        empty = false;
    }

    public float getX() {
        return x;
    }

    public void setX(float xa) {
        x = xa;
    }

    public float getY() {
        return y;
    }

    public void setY(float ya) {
        y = ya;
    }

    public float getValue() {
        return value;
    }

    public void setXY(float xa, float ya) {
        x = xa;
        y = ya;
    }

    public void translateXY(float t) {
        x += t;
        y += t;
    }

    public void translateXY(float t, float g) {
        x += t;
        y += g;
    }

    public void scaleXY(float t) {
        x = x * t;
        y = y * t;
    }

    public boolean isEmpty() {
        return empty;
    }

    public String toString() {
        return "(" + x + "," + y + "," + value + ")";
    }

}
