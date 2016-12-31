package edu.gmu.stc.climatespark.util

/**
  * Created by Fei Hu on 12/27/16.
  */
object RuntimeMonitor {

  def show_timing[T](proc: => T): T = {
    val start=System.nanoTime()
    val res = proc // call the code
    val end = System.nanoTime()
    println("Time elapsed: " + (end-start)/1000 + " microsecs")
    res
  }
}
