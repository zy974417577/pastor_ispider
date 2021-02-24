package com.pastor.streaming.launch

import com.pastor.common.util.database.ScalikeDBUtils
import com.pastor.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
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
import redis.clients.jedis.JedisCluster

/****************************************************************************
 * NAME:                IspiderStreaming                                    *
 * PURPOSE:           IspiderStreaming代码主程序                              *
 ****************************************************************************
 * REVISIONS:                                                               *
 * VER          DATE            AUTHOP               DESCRIPTION            *
 ********   ************    **************         ***************          *
 * 1.0       2021-02-23        zhaoyang           新增IspiderStreaming类     *
 ****************************************************************************
 *  explain:                                                                *
 *      TODO... 主程序入口类                                                  *
 *      1、使用广播变量动态匹配数据清洗规则
 ****************************************************************************/
object IspiderStreaming {

  def main(args: Array[String]): Unit = {
    //TODO... 设置日志级别
    LoggerLevels.setStreamingLogLevels()
    //TODO... 初始化操作
    val (zkHost, zkPath, topics, kafkaParams, ssc) = init()
    //TODO... 代码执行方法
    setUp(topics, kafkaParams, zkHost, zkPath, ssc)
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 初始化操作，初始化变量
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

    val sc = ssc.sparkContext
    //TODO... 初始zookeeper客户端
    val zkClint: ZkClient = new ZkClient(zkHost)
    //TODO... 读取保存在ZK上的offset
    val offset = KafkaOffsetUtil.readOffsets(zkClint, zkHost, zkPath, topics(0))
    val sql = "select value from nh_filter_rule"
    //TODO... 读取MySQL中的清洗规则规则,加入广播变量动态读数
    val ruleArr = ScalikeDBUtils.queryDB(sql)
    @volatile var broadcastValue = sc.broadcast(ruleArr)
    //TODO... 获取redis连接
    val jedis: JedisCluster = JedisConnectionUtil.getJedisCluster
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
      value.persist(StorageLevel.MEMORY_ONLY_SER)
      //TODO... IP访问量统计
      IpListCount.listCount(value)
      //TODO... 获取是否需要更新匹配规则
      val flag = jedis.get("FilterChangerFlag")

      if (!flag.isEmpty && flag.toBoolean) {
        //TODO... 清空广播变量
        broadcastValue.unpersist()
        //TODO... 读取MySQL新规则
        val newRule = ScalikeDBUtils.queryDB(sql)
        broadcastValue =sc.broadcast(newRule)

        jedis.set("FilterChangerFlag","fales")
      }
      //TODO... 提交offset
      KafkaOffsetUtil.saveOffsets(zkClint, zkHost, zkPath, rdd)
    })

  }
}
