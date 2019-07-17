package spark

import java.lang

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{Difftime, JedisConnectionPool, JedisOffset, KpiTools}

import scala.collection.mutable

/**
  * Redis管理Offset
  */
object KafkaRedisOffset {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("offset").setMaster("local[2]")
      // 设置没秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      // 设置序列化机制
      .set("spark.serlizer","org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf,Seconds(10))
    //将city文件进行广播
    val file = ssc.sparkContext.textFile("E:\\学习\\spark\\项目（二）01\\充值平台实时统计分析\\city.txt")
    val pair: RDD[(String, String)] = file.map(t => {
      val map = new mutable.HashMap[String,String]()
      val code = t.split("\\s")(0)
      val pro = t.split("\\s")(1)
      (code, pro)
    })
    val probroad: Broadcast[Array[(String, String)]] = ssc.sparkContext.broadcast(pair.collect())
    // 配置参数
    // 配置基本参数
    // 组名
    val groupId = "qfmrs"
    // topic
    val topic = "hz1903b"
    // 指定Kafka的broker地址（SparkStreaming程序消费过程中，需要和Kafka的分区对应）
    val brokerList = "192.168.110.111:9092"
    // 编写Kafka的配置参数
    val kafkas = Map[String,Object](
      "bootstrap.servers"->brokerList,
      // kafka的Key和values解码方式
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->groupId,
      // 从头消费
      "auto.offset.reset"-> "earliest",
      // 不需要程序自动提交Offset
      "enable.auto.commit"-> (false:lang.Boolean)
    )
    // 创建topic集合，可能会消费多个Topic
    val topics = Set(topic)
    // 第一步获取Offset
    // 第二步通过Offset获取Kafka数据
    // 第三步提交更新Offset
    // 获取Offset
    var fromOffset:Map[TopicPartition,Long] = JedisOffset(groupId)
    // 判断一下有没数据
    val stream :InputDStream[ConsumerRecord[String,String]] =
      if(fromOffset.size == 0){
        KafkaUtils.createDirectStream(ssc,
          // 本地策略
          // 将数据均匀的分配到各个Executor上面
          LocationStrategies.PreferConsistent,
          // 消费者策略
          // 可以动态增加分区
          ConsumerStrategies.Subscribe[String,String](topics,kafkas)
        )
      }else{
        // 不是第一次消费
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String,String](fromOffset.keys,kafkas,fromOffset)
        )
      }
    stream.foreachRDD({
      rdd=>
        val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 业务处理,json数据解析
        val data = rdd.map(_.value()).map(t => JSON.parseObject(t))
          //过滤支付通知数据
          .filter(_.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
          .map(t => {
            //判断是否充值成功
            val res = t.getString("bussinessRst")
            //如果充值成功，获取充值金额
            val money:Double = if(res.equals("0000")) t.getDouble("chargefee") else 0.0
            //充值成功数
            val feecount = if(res.equals("0000")) 1 else 0
            //开始充值时间
            val starttime = t.getString("requestId")
            val day = starttime.substring(0,8)
            val hour = starttime.substring(8,10)
            val minute = starttime.substring(10,12)
            //结束充值时间
            val stoptime = t.getString("receiveNotifyTime")
            //省份
            val provinceCode = t.getString("provinceCode")
            //时间差，时长
            val diftime = Difftime.difftime(starttime,stoptime)
            //（日期，小时，kpi（订单，成功订单，订单金额，订单时长），省份，分钟数）
            (day,hour,List[Double](1,feecount,money,diftime),provinceCode,minute)
          })

        /**
          * 业务概况（总订单量，成功订单量，总金额，总花费时长）
          */
        KpiTools.kpi_general(data)

        KpiTools.kpi_general_min(data)

        KpiTools.kpi_general_hour(data)

        KpiTools.kpi_general_top(data,probroad)

        KpiTools.kpi_general_hourCounts(data)


        // 将偏移量进行更新
        val jedis = JedisConnectionPool.getConnection()
        for (or<-offestRange){
          jedis.hset(groupId,or.topic+"-"+or.partition,or.untilOffset.toString)
        }
        jedis.close()
    })
    // 启动
    ssc.start()
    ssc.awaitTermination()
  }
}
