package com.pastor.streaming.launch


import com.pastor.common.bean.RequestType
import com.pastor.common.util.database.ScalikeDBUtils
import com.pastor.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import com.pastor.common.util.kafka.KafkaOffsetUtil
import com.pastor.common.util.log4j.LoggerLevels
import com.pastor.streaming.businessprocess.{DataSplit, EncryptedData, IpListCount, RequestTypeClassifier, URLFilter}
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

/** **************************************************************************
 * NAME:                IspiderStreaming                                    *
 * PURPOSE:           IspiderStreaming代码主程序                              *
 * ***************************************************************************
 * REVISIONS:                                                               *
 * VER          DATE            AUTHOP               DESCRIPTION            *
 * *******   ************    **************         ***************          *
 * 1.0       2021-02-23        zhaoyang           新增IspiderStreaming类     *
 * ***************************************************************************
 * explain:                                                                *
 *      TODO... 主程序入口类                                                  *
 * 1、使用广播变量动态匹配数据清洗规则
 * ***************************************************************************/
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
    //TODO... 读取MySQL中的清洗规则规则,加入广播变量动态读数
    val ruleArr = ScalikeDBUtils.queryDBMatch()
    @volatile var broadcastValue = sc.broadcast(ruleArr)
    // TODO... 读取MySQL中国际查询国外查询
    val ruleMap = ScalikeDBUtils.queryRuleMap()
    @volatile var ruleMapBroadcast= sc.broadcast(ruleMap)
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
      val valueRDD: RDD[String] = rdd.map(_.value())
      valueRDD.persist(StorageLevel.MEMORY_ONLY_SER)
      //TODO... IP访问量统计
      IpListCount.listCount(valueRDD)
      //TODO... 获取是否需要更新匹配规则
      val flag = jedis.get("FilterChangerFlag")
      if (!flag.isEmpty && flag.toBoolean) {
        //TODO... 清空广播变量
        broadcastValue.unpersist()
        //TODO... 读取MySQL新规则
        val newRule = ScalikeDBUtils.queryDBMatch()
        broadcastValue = sc.broadcast(newRule)
        jedis.set("FilterChangerFlag", "false")
      }
      //TODO... 是否要更新分类规则变更标识
      val ruleChangeFlagtr = jedis.get("ClassifyRuleChangeFlag")
      if(!ruleChangeFlagtr.isEmpty && ruleChangeFlagtr.toBoolean){
        ruleMapBroadcast.unpersist()
        val newRuleMapBroadcast = ScalikeDBUtils.queryRuleMap()
        ruleMapBroadcast = sc.broadcast(newRuleMapBroadcast)
        jedis.set("ClassifyRuleChangeFlag","false")
      }

      //TODO... 1、过滤数据，踢出掉不符合规则的数据
      val filterRDD = valueRDD.filter(messageRDD => URLFilter.filterURL(messageRDD, broadcastValue.value))
      //TODO... 2、数据脱敏
      val encryptionRDD = filterRDD.map(messageRDD => {
        //TODO... 2.1手机号脱敏
        val phoneStr = EncryptedData.encryptedPhone(messageRDD)
        //TODO... 2.2身份证脱敏
        val idRDD = EncryptedData.encryptedID(phoneStr)
        //TODO... 3数据拆分
        val (request, requestMethod, contentType, requestBody, httpReferrer, remoteAddr, httpUserAgent, timeIso8601, serverAddr, cookiesStr, cookieValue_JSESSIONID, cookieValue_USERID) = DataSplit.dateSplit(idRDD)
        //TODO... 4分类查询：判断是国际还国内分为四种情况：国内查询、国内预定、国际查询、国际预定
       val requestType:RequestType = RequestTypeClassifier.classifyByRequest(request,ruleMapBroadcast.value)
        requestType
        
      }).foreach(println(_))

      //TODO... 提交offset
      KafkaOffsetUtil.saveOffsets(zkClint, zkHost, zkPath, rdd)
    })

  }
}
