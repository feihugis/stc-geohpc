package edu.gmu.stc.climatespark.io.datastructure

/**
  * Created by Fei Hu on 4/3/17.
  */
case class ArrayBbox2D(state: String, corner0: Int, corner1: Int, shape0: Int, shape1: Int)

case class ArrayBbox3D(state: String, corner0: Int, corner1: Int, corner2: Int,
                                      shape0: Int, shape1: Int, shape2: Int)
