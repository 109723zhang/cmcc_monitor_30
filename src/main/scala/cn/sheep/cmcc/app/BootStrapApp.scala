package cn.sheep.cmcc.app

import java.text.SimpleDateFormat

import cn.sheep.cmcc.utils.{AppParams, Jpools, OffsetHandler}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 移动运营实时监控平台-- Monitor
  * sheep.Old @ 64341393
  * Created 2018/5/3
  */
object BootStrapApp {

    def main(args: Array[String]): Unit = {

        // spark streaming
        val sparkConf = new SparkConf()
          .setAppName("移动运营实时监控平台-- Monitor")
          .setMaster("local[*]")
          // 设置序列化方式， [rdd] [worker]
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          // 占用空间比较小
          .set("spark.rdd.compress", "true")
          // total delay
          .set("spark.streaming.kafka.maxRatePerPartition", "1000") // batchSize = partitionNum * 1000 * batchTime
          .set("spark.streaming.stopGracefullyOnShutdown", "true")

        val ssc = new StreamingContext(sparkConf, Seconds(2))

        // 获取数据库中存储的偏移量
        val currentOffset = OffsetHandler.getMydbCurrentOffset

        // 获取数据
        val stream = KafkaUtils.createDirectStream(ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](
                AppParams.topics,
                AppParams.kafkaParams,
                currentOffset)
        )


        stream.foreachRDD(rdd => {


            val baseData = rdd
              // ConsumerRecord => JSONObject
              .map(cr => JSON.parseObject(cr.value()))
              // 过滤出充值通知日志
              .filter(obj => obj.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
              .cache()

            val totalSucc = baseData.map(obj => {
                  val reqId = obj.getString("requestId")
                  // 获取日期
                  val day = reqId.substring(0, 8)

                  // 取出该条充值是否成功的的标志
                  val result = obj.getString("bussinessRst")
                  val flag = if (result.equals("0000")) 1 else 0

                  (day, flag)
              }).reduceByKey(_ + _)

            // wordcount => (word, 1) => reduceByKey()
            // 每天的充值成功订单量 => (20180503, 0) => reduceByKey(_ + _)

            val totalMoney = baseData.map(obj => {
                val reqId = obj.getString("requestId")
                // 获取日期
                val day = reqId.substring(0, 8)

                // 取出该条充值是否成功的的标志
                val result = obj.getString("bussinessRst")
                // 金额
                val fee: Double = if (result.equals("0000")) obj.getString("chargefee").toDouble else 0

                (day, fee)
            }).reduceByKey(_ + _)

            // 总订单量
            val total = baseData.count()

            val totalTime = baseData.map(obj => {
                val reqId = obj.getString("requestId")
                // 获取日期
                val day = reqId.substring(0, 8)

                // 取出该条充值是否成功的的标志
                val result = obj.getString("bussinessRst")
                val endTime = obj.getString("receiveNotifyTime")
                val starTime = reqId.substring(0, 17)

                val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
                val cost = if(result.equals("0000")) format.parse(endTime).getTime - format.parse(starTime).getTime else 0

                (day, cost)
            }).reduceByKey(_ + _)



            // 结果存入到redis
            totalSucc.foreachPartition(itr => {
                val jedis = Jpools.getJedis
                itr.foreach(tp =>
                    jedis.incrBy("S:"+tp._1, tp._2)
                )
                jedis.close()
            })




        })


        // 启动程序
        ssc.start()
        ssc.awaitTermination()
    }


}
