package com.pastor.streaming.businessprocess

import com.pastor.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

/****************************************************************************
 * NAME:                IpListCount                                         *
 * PURPOSE:            IP链路统计公共                                         *
 ****************************************************************************
 * REVISIONS:                                                               *
 * VER          DATE            AUTHOP               DESCRIPTION            *
 ********   ************    **************         ***************          *
 * 1.0       2021-02-21        zhaoyang           新增IpListCount公共类       *
 ****************************************************************************
 *  explain:                                                                *
 *      TODO... IpListCount公共类，用于链路统计                                 *
 ****************************************************************************/
object IpListCount {
  /**
   * 链路统计方法
   * @param rdd
   */
  def listCount(rdd:RDD[String]): Unit ={

//  TODO 统计ip访问次数
    val ipCount = rdd.map(message => {
      val messageArray = message.split("#CS#")
      var ip = ""
      if (messageArray.length > 9) {
        ip = messageArray(9)
      }
      (ip, 1)
    }).reduceByKey(_ + _)

//  TODO 获取当前最大活跃数
    val activeCount = rdd.map(message => {
      val messageArray = message.split("#CS#")
      var ip = ""
      var activeuser = ""
      if (messageArray.length > 11) {
        ip = messageArray(9)
        activeuser = messageArray(11)
      }
      (ip, activeuser)
    }).reduceByKey((_, y) => y)

//    TODO 写入redis
    if(!activeCount.isEmpty() && !ipCount.isEmpty()){
      val ipCountMap = ipCount.collectAsMap()
      val jedis = JedisConnectionUtil.getJedisCluster
      val activeCountMap = activeCount.collectAsMap()
      val IpActiveMap = Map(
        "serverCountMap" -> ipCountMap,
        "activeNumMap" -> activeCountMap
      )
      val key=PropertiesUtil.getStringByKey("cluster.key.monitor.linkProcess","jedisConfig.properties")+System.currentTimeMillis().toString
      val seconds=PropertiesUtil.getStringByKey("cluster.exptime.monitor","jedisConfig.properties").toInt
      println(Json(DefaultFormats).write(IpActiveMap))
      jedis.setex(key,seconds,Json(DefaultFormats).write(IpActiveMap))
    }
  }
}
