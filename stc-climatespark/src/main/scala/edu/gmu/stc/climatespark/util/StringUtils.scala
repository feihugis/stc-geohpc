package edu.gmu.stc.climatespark.util

/**
  * Created by Fei Hu on 1/9/17.
  */
object StringUtils {

  def getHostID(hostName: String) = hostName.substring(hostName.size - 2, hostName.size).replace("U", "").toInt

}
