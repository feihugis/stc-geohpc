package edu.gmu.stc.climatespark.io.datastructure

/**
  * Created by Fei Hu on 11/18/16.
  */
abstract class MultiCell extends Serializable{

}

object MultiCell {
  def factory(lat: Float, lon: Float, time: Int, values: List[Short]):MultiCell = {
    values.length match {
        case 3 => new MultiCell3(lat, lon, time, values(0), values(1), values(2))
        case 9 => new MultiCell9(lat, lon, time, values(0), values(1), values(2),
                                                 values(3), values(4), values(5),
                                                 values(6), values(7), values(8))
        case _ => new MultiCell3(lat, lon, time, values(0), values(1), values(2))
    }
  }
}
