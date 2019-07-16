package utils

import java.text.SimpleDateFormat


object Difftime {

  private val fromat =new SimpleDateFormat("yyyyMMddHHmmssSSS")

  def difftime(starttime:String, endtime: String): Long = {
    val start = starttime.substring(0,17)
    fromat.parse(endtime).getTime - fromat.parse(start).getTime
  }

}
