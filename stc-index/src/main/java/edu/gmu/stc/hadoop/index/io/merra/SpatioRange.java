package edu.gmu.stc.hadoop.index.io.merra;

public class SpatioRange {
	public int[] corner;
	public int[] shape;
	
	public SpatioRange(int[] inCorner, int[] inShape) {
		corner = new int[inCorner.length];
		shape = new int[inShape.length];
		
		for(int i=0; i<corner.length; i++) {
			corner[i] = inCorner[i];
		}
		
		for(int i=0; i<shape.length; i++) {
			shape[i] = inShape[i];
		}
	}
}
