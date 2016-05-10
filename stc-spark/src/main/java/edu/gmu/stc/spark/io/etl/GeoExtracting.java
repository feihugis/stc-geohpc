package edu.gmu.stc.spark.io.etl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.jute.Index;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import edu.gmu.stc.datavisualization.netcdf.test.NetCDFManager;
import edu.gmu.stc.hadoop.raster.DataChunk;
import edu.gmu.stc.hadoop.raster.hdf5.ArrayFloatSerializer;
import edu.gmu.stc.hadoop.raster.hdf5.ArrayIntSerializer;
import edu.gmu.stc.hadoop.raster.hdf5.H5Chunk;
import edu.gmu.stc.hadoop.vector.Point;
import edu.gmu.stc.hadoop.vector.Polygon;
import edu.gmu.stc.hadoop.vector.Rectangle;
import edu.gmu.stc.hadoop.vector.extension.CountyFeature;
import edu.gmu.stc.hadoop.verctor.dataformat.geojson.CountyMultiPolygonJSON;
import edu.gmu.stc.hadoop.verctor.dataformat.geojson.CountyPolygonJSON;
import scala.Tuple2;
import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import ucar.ma2.Index2D;
import ucar.ma2.Index3D;

/**
 * Created by Fei Hu on 3/17/16.
 */
public class GeoExtracting {
  //TODO fix the max_value, it should be varied for different variables
  final static float max_value = 10000000.0f;
  final static float image_fill_value = -9999.0f;
  public static class GeoJSONFilter implements Function<String, Boolean> {
    String[] stateNames = null;
    boolean isObject = false;

    public GeoJSONFilter(String[] stateNames, boolean isObject) {
      this.stateNames = stateNames;
      this.isObject = isObject;
    }

    @Override
    public Boolean call(String v1) throws Exception {
      if (((v1.contains("\"Polygon\"") || v1.contains("\"MultiPolygon\"")) )) {
        if (stateNames == null) {
          return true;
        } else {
          for (int i = 0; i < stateNames.length; i++) {
            if (v1.contains(stateNames[i])) {
              return isObject;
            }
          }
          return !isObject;
        }
      } else {
        return false;
      }
    }
  }

  public static class GeoFeatureFactory implements PairFunction<String, String, CountyFeature> {

    @Override
    public Tuple2<String, CountyFeature> call(String in) throws Exception {
      Gson gson = new GsonBuilder().create();
      if (in.contains("\"Polygon\"")) {
        CountyPolygonJSON countyPlgn = gson.fromJson(in, CountyPolygonJSON.class);
        CountyFeature feature = new CountyFeature(countyPlgn.getType(), countyPlgn.getProperties().getGEO_ID(),
                                                  countyPlgn.getProperties().getSTATE(),
                                                  countyPlgn.getProperties().getCOUNTY(),
                                                  countyPlgn.getProperties().getNAME(),
                                                  countyPlgn.getProperties().getLSAD(),
                                                  countyPlgn.getProperties().getCENSUSAREA(),
                                                  countyPlgn.getGeometry().toPolygon());
        return new Tuple2<String, CountyFeature>(feature.getSTATE(), feature);
      }

      if (in.contains("\"MultiPolygon\"")) {
        CountyMultiPolygonJSON countyPlgn = gson.fromJson(in, CountyMultiPolygonJSON.class);
        CountyFeature feature = new CountyFeature(countyPlgn.getType(), countyPlgn.getProperties().getGEO_ID(),
                                                  countyPlgn.getProperties().getSTATE(),
                                                  countyPlgn.getProperties().getCOUNTY(),
                                                  countyPlgn.getProperties().getNAME(),
                                                  countyPlgn.getProperties().getLSAD(),
                                                  countyPlgn.getProperties().getCENSUSAREA(),
                                                  countyPlgn.getGeometry().toMultiPolygons());
        return new Tuple2<String, CountyFeature>(feature.getSTATE(), feature);
      }
      return null;
    }
  }

  public static class ChunkMaskFactory implements PairFunction<CountyFeature, H5Chunk, ArrayIntSerializer> {
    double xResolution; //MetaData.MERRA2.lonUnit;
    double yResolution;//MetaData.MERRA2.latUnit;
    double x_orig = -180.0; //MetaData.MERRA2.lon_orig;
    double y_orig = 90.0; //MetaData.MERRA2.lat_orig;

    public ChunkMaskFactory(double xResolution, double yResolution,
                            double x_orig, double y_orig) {
      this.xResolution = xResolution;
      this.yResolution = yResolution;
      this.x_orig = x_orig;
      this.y_orig = y_orig;
    }

    @Override
    public Tuple2<H5Chunk, ArrayIntSerializer> call(CountyFeature feature) throws Exception {
      Rectangle bbox = feature.getMBR().toLogicView(xResolution, yResolution, x_orig, y_orig);
      int[] corners = new int[]{(int) bbox.getMinY(), (int) bbox.getMinX()};
      int[] shape = new int[]{((int) bbox.getMaxY() - (int) bbox.getMinY() + 1), ((int) bbox.getMaxX() - (int) bbox.getMinX() + 1)};
      H5Chunk stateInfo = new H5Chunk();
      stateInfo.setCorner(corners);
      stateInfo.setShape(shape);
      int[] array = new int[shape[0] * shape[1]];

      //fill the boundary into array mask
      for (int c=0; c<feature.getFeature().size(); c++) {
        Polygon polygon = feature.getFeature().get(c).toLogicView(xResolution, yResolution, x_orig, y_orig);
        for (int i = 0; i < polygon.getNpoints(); i++) {
          int py = (int) polygon.getYpoints()[i];
          int px = (int) polygon.getXpoints()[i];
          array[(py - corners[0]) * shape[1] + (px - corners[1])] = 1;
        }
      }

      // fill the points in the boundary into the array mask
      for (double y = ((int) bbox.getMinY())+0.0; y < bbox.getMaxY(); y++) {
        for(double x = ((int) bbox.getMinX())+0.0; x < bbox.getMaxX(); x++) {
          Point point = new Point(x, y);
          for (int c=0; c<feature.getFeature().size(); c++) {
            Polygon polygon = feature.getFeature().get(c).toLogicView(xResolution, yResolution, x_orig, y_orig);
            if (polygon.contains(point.x, point.y)) {
              array[(((int) y) - corners[0])*shape[1] + (((int) x) - corners[1])] = 1;
            }
          }
        }
      }

      return new Tuple2<H5Chunk, ArrayIntSerializer>(stateInfo, new ArrayIntSerializer(shape, array));
    }
  }

  public static Tuple2<H5Chunk, ArrayIntSerializer> combineChunkMasks (List<Tuple2<H5Chunk, ArrayIntSerializer>> maskList) {
    int x_min = Integer.MAX_VALUE, x_max = Integer.MIN_VALUE, y_min = Integer.MAX_VALUE, y_max = Integer.MIN_VALUE;

    for (Tuple2<H5Chunk, ArrayIntSerializer> tuple2 : maskList) {
      int[] corner = tuple2._1().getCorner();
      int[] shape = tuple2._1().getShape();
      if (x_min>corner[1]) x_min = corner[1];
      if (y_min>corner[0]) y_min = corner[0];
      if (x_max<corner[1]+shape[1]) x_max=corner[1]+shape[1];
      if (y_max<corner[0]+shape[0]) y_max=corner[0]+shape[0];
    }

    int[] cornerMask = new int[] {y_min, x_min};
    int[] shapeMask = new int[] {y_max-y_min+1, x_max-x_min+1};
    int[] data = new int[shapeMask[0]*shapeMask[1]];
    ArrayIntSerializer maskArray = new ArrayIntSerializer(shapeMask,data);
    Index2D maskIndex = new Index2D(shapeMask);

    for (Tuple2<H5Chunk, ArrayIntSerializer> tuple2 : maskList) {
      int[] corner = tuple2._1().getCorner();
      int[] shape = tuple2._1().getShape();
      Index2D chunkIndex = new Index2D(shape);
      for (int y = 0; y < shape[0]; y++) {
        for (int x = 0; x < shape[1]; x++) {
          chunkIndex.set(y,x);
          maskIndex.set(corner[0] - cornerMask[0] + y, corner[1] - cornerMask[1] + x);
          //avoid the overlay between the bbox
          if (maskArray.getArray().get(maskIndex) > 0) {
            continue;
          }
          maskArray.getArray().set(maskIndex, tuple2._2().getArray().get(chunkIndex));
        }
      }
    }

    H5Chunk chunk = new H5Chunk();
    chunk.setCorner(cornerMask);
    chunk.setShape(shapeMask);
    Tuple2<H5Chunk, ArrayIntSerializer> mask = new Tuple2<H5Chunk, ArrayIntSerializer>(chunk, maskArray);
    return mask;
  }


  public static class ChunkExtractingByMask implements PairFunction<Tuple2<DataChunk, ArrayFloatSerializer>, DataChunk, ArrayFloatSerializer> {
    Broadcast<Tuple2<H5Chunk, ArrayIntSerializer>> mask;

    public ChunkExtractingByMask(Broadcast<Tuple2<H5Chunk, ArrayIntSerializer>> mask) {
      this.mask = mask;
    }

    @Override
    public Tuple2<DataChunk, ArrayFloatSerializer> call(Tuple2<DataChunk, ArrayFloatSerializer> tuple2) throws Exception {
        int[] maskCorner = mask.value()._1().getCorner();
        int[] maskShape = mask.value()._1().getShape();
        Rectangle maskBBox = new Rectangle(maskCorner[1], maskCorner[0], maskCorner[1]+maskShape[1]-1, maskCorner[0]+maskShape[0]-1);

        ArrayFloatSerializer v1 = tuple2._2();
        int[] shape = tuple2._1().getShape().clone();
        int[] corner = tuple2._1().getCorner().clone();
        Rectangle chunkBBox = new Rectangle(corner[corner.length-1], corner[corner.length-2], corner[corner.length-1]+shape[corner.length-1]-1, corner[corner.length-2]+shape[corner.length-2]-1);

        if (chunkBBox.isIntersected(maskBBox)) {
          Rectangle intectedRec = chunkBBox.getIntersection(maskBBox);
          shape[shape.length-2] = (int) (intectedRec.getHeight())+1;
          shape[shape.length-1] = (int) (intectedRec.getWidth())+1;
          corner[corner.length-2] = (int) intectedRec.getMinY();
          corner[corner.length-1] = (int) intectedRec.getMinX();

          ArrayFloat result = (ArrayFloat) Array.factory(float.class, shape);
          ucar.ma2.Index index = ucar.ma2.Index.factory(shape);
          ucar.ma2.Index rawIndex = ucar.ma2.Index.factory(tuple2._2().getArray().getShape());
          int[] rawCorner = tuple2._1().getCorner();
          ucar.ma2.Index maskIndex = ucar.ma2.Index.factory(mask.value()._2().getArray().getShape());

          switch (index.getRank()) {
            case 3:
              for (int t=0; t<shape[0]; t++) {
                for (int y=0; y<shape[1]; y++) {
                  for (int x=0; x<shape[2]; x++) {
                    index.set(t,y,x);
                    if (maskBBox.contains(corner[2] + x, corner[1] + y)) {
                      float isContain = mask.value()._2.getArray().get(maskIndex.set(corner[1] + y - maskCorner[0], corner[2] + x - maskCorner[1]));
                      if (isContain>0) {
                        if (v1.getArray().get(rawIndex.set(t, rawIndex.getShape(1)-y-1, x))>max_value) {
                          result.set(index, GeoExtracting.image_fill_value);
                          //continue;
                        }
                        result.set(index,v1.getArray().get(rawIndex.set(t, /*rawIndex.getShape(1)-y-1*/y+corner[1]-rawCorner[1], x+corner[2]-rawCorner[2])));
                      } else {
                        result.set(index, GeoExtracting.image_fill_value);
                      }
                    } else {
                      result.set(index, GeoExtracting.image_fill_value);
                    }
                  }
                }
              }
              break;
          }

          tuple2._1().setCorner(corner);
          tuple2._1().setShape(shape);
          return new Tuple2<DataChunk, ArrayFloatSerializer>(tuple2._1(), new ArrayFloatSerializer(result.getShape(), (float[]) result.getStorage()));
        } else {
          return null;
        }
      }
    }

  public static class InterpolateArrayByIDW implements PairFunction<Tuple2<DataChunk, ArrayFloatSerializer>, DataChunk, ArrayFloatSerializer> {
    int zoomScale = 1;

    public InterpolateArrayByIDW(int zoomScale) {
      this.zoomScale = zoomScale;
    }

    @Override
    public Tuple2<DataChunk, ArrayFloatSerializer> call( Tuple2<DataChunk, ArrayFloatSerializer> tuple2) throws Exception {
      String key = tuple2._1.getVarShortName() + tuple2._1().getCorner()[0] + "_" + tuple2._1().getFilePath().split("\\.")[2];
      int[] shape = tuple2._1().getShape();
      int[] corner = tuple2._1().getCorner();

      if (zoomScale == 1) {
        return tuple2;
      }

      NetCDFManager manager = new NetCDFManager();
      ArrayFloat.D2 d2 = (ArrayFloat.D2) tuple2._2().getArray().reduce(0);
      ArrayFloat.D2 input = manager.interpolateArray(d2, zoomScale, 2.0f);

      shape[shape.length-2] = shape[shape.length-2]*zoomScale;
      shape[shape.length-1] = shape[shape.length-1]*zoomScale;
      corner[corner.length-2] = corner[corner.length-2]*zoomScale;
      corner[corner.length-1] = corner[corner.length-1]*zoomScale;
      tuple2._1().setCorner(corner);
      tuple2._1().setShape(shape);
      return new Tuple2<DataChunk, ArrayFloatSerializer>(tuple2._1(), new ArrayFloatSerializer(shape, (float[]) input.getStorage()));
    }
  }

  public static class CombineChunks implements PairFunction<Tuple2<String, Iterable<Tuple2<DataChunk, ArrayFloatSerializer>>>, String, ArrayFloatSerializer> {
    Broadcast<Tuple2<H5Chunk, ArrayIntSerializer>> mask;
    int zoomScale;

    public CombineChunks( Broadcast<Tuple2<H5Chunk, ArrayIntSerializer>> mask, int zoomScale) {
      this.mask = mask;
      this.zoomScale = zoomScale;
    }

    @Override
    public Tuple2<String, ArrayFloatSerializer> call( Tuple2<String, Iterable<Tuple2<DataChunk, ArrayFloatSerializer>>> stringIterableTuple2) throws Exception {
      int height = mask.value()._1().getShape()[0]*zoomScale;
      int width = mask.value()._1().getShape()[1]*zoomScale;
      int[] cornerMask = mask.value()._1().getCorner().clone();
      cornerMask[0] = cornerMask[0]*zoomScale;
      cornerMask[1] = cornerMask[1]*zoomScale;
      float[] defaultValues = new float[height*width];
      for (int i=0; i<height; i++) {
        for (int j=0; j<width; j++) {
          defaultValues[i*width + j] = 100.0f;
        }
      }
      ArrayFloat combinedArray = (ArrayFloat) Array.factory(float.class, new int[]{height, width}, defaultValues);
      Index2D maskIndex = new Index2D(new int[]{height, width});

      Iterator<Tuple2<DataChunk, ArrayFloatSerializer>> itor = stringIterableTuple2._2().iterator();

      while (itor.hasNext()) {
        Tuple2<DataChunk, ArrayFloatSerializer> tuple = itor.next();
        int[] corner = tuple._1().getCorner();
        int[] shape = tuple._1().getShape();
        Index3D chunkIndex = new Index3D(shape);
        for (int y = corner[1]; y< corner[1] + shape[1]; y++) {
          for (int x = corner[2]; x < corner[2] + shape[2]; x++) {
            maskIndex.set(y-cornerMask[0], x-cornerMask[1]);
            combinedArray.set(maskIndex,tuple._2().getArray().get(chunkIndex.set(0, y-corner[1], x-corner[2])));
          }
        }
      }

      return new Tuple2<String, ArrayFloatSerializer>(stringIterableTuple2._1(),
                                                      new ArrayFloatSerializer(combinedArray.getShape(),
                                                                               (float[]) combinedArray.getStorage()));
    }
  }
}
