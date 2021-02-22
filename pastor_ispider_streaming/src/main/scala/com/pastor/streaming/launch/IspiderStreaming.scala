package com.pastor.streaming.launch

import com.pastor.common.util.jedis.PropertiesUtil
import com.pastor.common.util.kafka.KafkaOffsetUtil
import com.pastor.common.util.log4j.LoggerLevels
import com.pastor.streaming.businessprocess.IpListCount
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object IspiderStreaming {

  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels()

    val (zkHost, zkPath, topics, kafkaParams, ssc) = init()

    setUp(topics, kafkaParams, zkHost, zkPath, ssc)


    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 初始化操作
   *
   * @return
   */
  def init(): (String, String, Array[String], Map[String, Object], StreamingContext) = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("IspiderStreaming")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(10))

    val zkHost = PropertiesUtil.getStringByKey("zookeeper.hosts", "zookeeperConfig.properties")
    val zkPath = PropertiesUtil.getStringByKey("dataprocess.zkPath", "zookeeperConfig.properties")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "Ispider_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(PropertiesUtil.getStringByKey("source.nginx.topic", "kafkaConfig.properties"))

    (zkHost, zkPath, topics, kafkaParams, ssc)
  }


  /**
   * 代码执行函数
   *
   * @param topics      主题
   * @param kafkaParams kafka配置
   * @param zkHost      zk地址
   * @param zkPath      zk路径
   * @param ssc         sparkStreaming
   */
  def setUp(topics: Array[String], kafkaParams: Map[String, Object], zkHost: String, zkPath: String, ssc: StreamingContext): Unit = {

    //TODO... 初始zookeeper客户端
    val zkClint: ZkClient = new ZkClient(zkHost)

    //TODO... 读取保存在ZK上的offset
    val offset = KafkaOffsetUtil.readOffsets(zkClint, zkHost, zkPath, topics(0))


    if (null == offset) {
      offset.get.keySet.foreach(x => {
        println(x.partition() + " : " + x.topic())
      })
    }
    //TODO... 获取stream
    val dateStream: InputDStream[ConsumerRecord[String, String]] = offset match {
      //TODO... 1、未读取到offset
      case None => KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams))
      //TODO... 2、读取到offset
      case Some(formOffsets) => KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams, formOffsets))
    }


    dateStream.foreachRDD(rdd => {
      val value: RDD[String] = rdd.map(_.value())
      //    TODO... IP访问量统计
      value.persist(StorageLevel.MEMORY_ONLY_SER)
      IpListCount.listCount(value)

      //    TODO... 提交offset
      KafkaOffsetUtil.saveOffsets(zkClint, zkHost, zkPath, rdd)
    })

  }
}
