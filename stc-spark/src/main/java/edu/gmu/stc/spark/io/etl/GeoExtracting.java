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

import java.io.Serializable;
import java.util.List;

import edu.gmu.stc.hadoop.raster.hdf5.ArrayIntSerializer;
import edu.gmu.stc.hadoop.raster.hdf5.H5Chunk;
import edu.gmu.stc.hadoop.vector.Point;
import edu.gmu.stc.hadoop.vector.Polygon;
import edu.gmu.stc.hadoop.vector.Rectangle;
import edu.gmu.stc.hadoop.vector.extension.CountyFeature;
import edu.gmu.stc.hadoop.verctor.dataformat.geojson.CountyMultiPolygonJSON;
import edu.gmu.stc.hadoop.verctor.dataformat.geojson.CountyPolygonJSON;
import scala.Tuple2;
import ucar.ma2.Index2D;

/**
 * Created by Fei Hu on 3/17/16.
 */
public class GeoExtracting {
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

      for (int c=0; c<feature.getFeature().size(); c++) {
        Polygon polygon = feature.getFeature().get(c).toLogicView(xResolution, yResolution, x_orig, y_orig);
        for (int i = 0; i < polygon.getNpoints(); i++) {
          int py = (int) polygon.getYpoints()[i];
          int px = (int) polygon.getXpoints()[i];
          array[(py - corners[0]) * shape[1] + (px - corners[1])] = 1;
        }
      }

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
}
