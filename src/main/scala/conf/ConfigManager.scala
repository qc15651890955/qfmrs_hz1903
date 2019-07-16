package conf

import java.util.Properties

object ConfigManager {
  private val prop = new Properties()
  try{
    val in_basic= ConfigManager.getClass.getClassLoader.getResourceAsStream("basic.properties");
    prop.load(in_basic)
  }catch {
    case e:Exception=>e.printStackTrace()
  }

  def getProper(key:String):String={
    prop.getProperty(key)
  }
}
