package cn.sheep.cmcc.utils

import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import scalikejdbc._
import scalikejdbc.config._

/**
  * 改程序所用到的相关参数
  * sheep.Old @ 64341393
  * Created 2018/5/3
  */
object AppParams {

    /**
      * 解析application.conf
      */
    private lazy val load: Config = ConfigFactory.load()

    // 订阅的主题
    val topics = load.getString("kafka.topics").split(",")

    // kafka集群所在的主机及端口
    val brokers = load.getString("kafka.broker.list")

    // 消费者的id
    val groupId = load.getString("kafka.group.id")

    // kafka的相关参数
    val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> brokers,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> groupId,
        "auto.offset.reset" -> "earliest",
        "enable.auto.commit" -> "false"
    )

    // redis主机
    val redisHost = load.getString("redis.host")
    val selectDBIndex = load.getInt("redis.db.index")

    import scala.collection.JavaConversions._
    // 省份code和name映射关系
    val pcode2PName = load.getObject("pcode2pname").unwrapped().toMap




}
