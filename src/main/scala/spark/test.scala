package spark

import conf.ConfigManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("11")
    val sc = new SparkContext(conf)
    val file = sc.textFile("E:\\学习\\spark\\项目（二）01\\充值平台实时统计分析\\city.txt")
    val pair: RDD[(String, String)] = file.map(t => {
      val code = t.split("\\s")(0)
      val pro = t.split("\\s")(1)
      (code, pro)
    })

    val broad = sc.broadcast(pair.collect())
  }
}
