package utils

import org.apache.spark.broadcast.Broadcast
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
          jedis.hincrBy("A-" + f._1, "costtime", f._2(3).toLong)
        })
        jedis.close()
      })
  }

  /**
    * 每分钟的订单量数据
    *
    */

  def kpi_general_min(baseDate: RDD[(String,String,List[Double],String,String)]): Unit = {
    baseDate.map(t => ((t._1,t._2,t._5),List(t._3(1),t._3(2)))).reduceByKey((list1,list2) => {
      list1.zip(list2).map(x => x._1 + x._2)
    })
      .foreachPartition(partition => {
        val jedis = JedisConnectionPool.getConnection()
        partition.foreach(f => {
          jedis.hincrBy("B-" + f._1, "total", f._2(0).toLong)
          jedis.hincrByFloat("B-" + f._1, "money", f._2(1))
        })
      })
  }

  /**
    *统计每小时各个省份的充值失败数据量
    */
  def kpi_general_hour(baseDate: RDD[(String,String,List[Double],String,String)]): Unit = {
    baseDate.map(t => ((t._4,t._1,t._2), t._3)).reduceByKey((list1,list2) => {
      list1.zip(list2).map(x => x._1 + x._2)
    })
      .foreachPartition(partition => {
        val connection = ConnectPoolUtils.getConnections()
        partition.foreach(f => {
          val sql = "insert into prohour(pro,hour,counts)" + "values('"+ f._1._1 + "','"+ f._1._3 + "','" + (f._2(0)-f._2(1)) + "')"
          val statement = connection.createStatement()
          statement.executeUpdate(sql)
        })
        ConnectPoolUtils.resultConn(connection)
      })
  }

  /**
    * 以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，只保留一位小数
    */
    def kpi_general_top(baseDate: RDD[(String,String,List[Double],String,String)],probroad: Broadcast[Array[(String, String)]]):Unit = {
      val connection = ConnectPoolUtils.getConnections()
    baseDate.map(t => (t._4,List(t._3(0),t._3(1)))).reduceByKey((list1,list2) => {
      list1.zip(list2).map(x => x._1 + x._2)
    }).sortBy(_._2(1),false).take(10)
        .foreach(x => {
          val code = probroad.value.toMap.getOrElse(x._1.toString,"0000")
          val percent:Double = x._2(1) / x._2(0)
          val sql = "insert into protop(code,pro,precent)" + "values('" + code + "','" + x._1 + "','" + percent + "')"
          val statement = connection.createStatement()
          statement.executeUpdate(sql)
        })
      ConnectPoolUtils.resultConn(connection)
  }

  /**
    * 实时统计每小时的充值笔数和充值金额
    */
    def kpi_general_hourCounts(baseDate: RDD[(String,String,List[Double],String,String)]): Unit = {
      baseDate.map(t => ((t._1,t._2),t._3)).reduceByKey((list1,list2) => {
        list1.zip(list2).map(x => x._1 + x._2)
      })
        .foreachPartition(partition => {
          val connection = ConnectPoolUtils.getConnections()
          partition.foreach(f => {
            val sql = "insert into hourCounts(date,hour,counts,money)" + "values('" + f._1._1 + "','" + f._1._2 + "','" + f._2(0) + "','" + f._2(2) + "')"
            val statement = connection.createStatement()
            statement.executeUpdate(sql)
          })
          ConnectPoolUtils.resultConn(connection)
        })
    }

}
