package utils

import org.apache.spark.rdd.RDD

object KpiTools {

  /**
    * 业务概况（总订单量，成功订单量，总金额，总花费时长）
    *
    */
  def kpi_general(baseDate: RDD[(String, String, List[Double], String, String)]): Unit = {
    baseDate.map(t => (t._1 , t._3)).reduceByKey((list1,list2) => {
      list1.zip(list2).map(x => x._1 + x._2)
    })
      .foreachPartition(partition => {
        val jedis = JedisConnectionPool.getConnection()
        partition.foreach(f => {
          jedis.hincrBy("A-" + f._1, "total", f._2(0).toLong)
          jedis.hincrBy("A-" + f._1, "success", f._2(1).toLong)
          jedis.hincrByFloat("A-" + f._1, "money", f._2(2))
          jedis.hincrBy("A-" + f._1, "diftime", f._2(3).toLong)
        })
        jedis.close()
      })
  }
}
