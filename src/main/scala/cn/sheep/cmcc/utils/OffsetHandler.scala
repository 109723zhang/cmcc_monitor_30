package cn.sheep.cmcc.utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

/**
  * sheep.Old @ 64341393
  * Created 2018/5/3
  */
object OffsetHandler {

    // 解析mysql连接配置信息
    DBs.setup()


    // 获取自己存储的偏移量信息， MySQL
    def getMydbCurrentOffset = {
        DB.readOnly{implicit session =>
            SQL("select * from streaming_offset_30 where groupId=?").bind(AppParams.groupId)
              .map(rs =>
                  (
                    new TopicPartition(rs.string("topicName"), rs.int("partitionId")),
                    rs.long("offset")
                  )
              ).list().apply().toMap
        }
    }

    /**
      * 持久化当前批次的偏移量
      * @param offsetRanges
      */
    def saveCurrentBatchOffset(offsetRanges: Array[OffsetRange]) = {
        DB.localTx{ implicit session =>
            offsetRanges.foreach(or => {
                SQL("replace into streaming_offset_30 values(?,?,?,?)")
                  .bind(or.topic, or.partition, or.untilOffset, AppParams.groupId)
                  .update()
                  .apply()
            })
        }

    }



}
