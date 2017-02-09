package edu.gmu.stc.climatespark.util

/**
  * Created by Fei Hu on 12/27/16.
  */
object RuntimeMonitor {

  def show_timing_result[T](proc: => T): (T, Long) = {
    val start=System.nanoTime()
    val res = proc // call the code
    val end = System.nanoTime()
    //println("Time elapsed: " + (end-start)/1000 + " microsecs")
    (res, (end-start)/1000)
  }

  def show_timing[T](proc: => T): Long = {
    val start=System.nanoTime()
    val res = proc // call the code
    val end = System.nanoTime()
    println("Time elapsed: " + (end-start)/1000 + " microsecs")
    (end - start)/1000
  }

  def show_multi_runtiming[T](proc: => T, repeatTime: Int): Long = {
    var time = 0L
    for (i <- 0 until repeatTime) {
      val start=System.nanoTime()
      val res = proc // call the code
      val end = System.nanoTime()
      time = time + end - start
    }

    time/repeatTime/1000
  }
}
