package cn.sheep.cmcc.app

import cn.sheep.cmcc.utils.{AppParams, KpiTools, OffsetHandler}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 移动运营实时监控平台-- Monitor
  * sheep.Old @ 64341393
  * Created 2018/5/3
  */
object BootStrapAppV2 {

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
          .set("spark.streaming.kafka.maxRatePerPartition", "15000") // batchSize = partitionNum * 1000 * batchTime
          .set("spark.streaming.stopGracefullyOnShutdown", "true")

        val ssc = new StreamingContext(sparkConf, Seconds(2))

        // 广播省份映射关系
        val pcode2PName: Broadcast[Map[String, AnyRef]] = ssc.sparkContext.broadcast(AppParams.pcode2PName)


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
            // 获取偏移量
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            val baseData = KpiTools.baseDataRDD(rdd)
            // 计算业务概况
            KpiTools.kpi_general(baseData)
            KpiTools.kpi_general_hour(baseData)
            // 业务质量
            KpiTools.kpi_quality(baseData, pcode2PName)
            // 实时充值情况分布
            KpiTools.kpi_realtime_minute(baseData)

            // 存储偏移量
            OffsetHandler.saveCurrentBatchOffset(offsetRanges)

        })


        // 启动程序
        ssc.start()
        ssc.awaitTermination()
    }


}
