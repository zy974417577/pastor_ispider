package com.pastor.common.util.hdfs

import java.text.SimpleDateFormat

import com.pastor.common.util.jedis.PropertiesUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import redis.clients.jedis.JedisCluster

import scala.collection.mutable.ArrayBuffer

/**
  * 将HDFS中的黑名单的数据存到Redis中，有效时间为1小时
  */
object BlackListToRedis {
  /**
    *判断指定redis库中的表是否为空，为空则进行设置HDFS中目录的标识
    *
    * @param jedis：通过jedis对redis进行操作
    * @param sc：传入的sparkContext
    * @param sqlContext ：传入的SQLContext
    */
  def blackListDataToRedis(jedis: JedisCluster,sc: SparkContext,sqlContext: SQLContext): Unit = {
    val dangKey = jedis.get("dang")
    if(dangKey == "no"){
      jedis.set("dang","no")
    }else {
      val arrayBuffer: ArrayBuffer[String] = ArrayBuffer()
      //获取hdfs的FileSystem
      val conf = new Configuration()
      //配置hdfs的路径
      conf.set("fs.defaultFS", "hdfs://192.168.56.111:8020");
      val fs = FileSystem.get(conf)
      //时间格式化类，用于按时间存储
      val sdf = new SimpleDateFormat("yyyy/MM/dd/HH")
      //获取当前时间
      val sysCurrentTime = System.currentTimeMillis()
      //添加hdfs路径到arrayBuffer
      for (i <- 0 to 1) {
        //为了避免有效数据丢失，所以从前一个小时之前进行取数据
        val startTime = sdf.format(sysCurrentTime - 1000 * 60 * 60 * i)
        val path = PropertiesUtil.getStringByKey("blackListPath", "HDFSPathConfig.properties") + startTime
        if (fs.exists(new Path(path))) {
          arrayBuffer += path
        }
      }
      //将黑名单数据向Redis中进行插入
      dataToRedis(sqlContext, arrayBuffer.toArray, jedis)
    }
  }

  /**
    * 创建DataFrame
    *
    * @param sqlContext
    * @param paths：HDFS路径
    * @param jedis
    */
  def dataToRedis(sqlContext:SQLContext,paths:Array[String],jedis: JedisCluster): Unit ={
      val parquet1: DataFrame = sqlContext.read.parquet(paths:_*)
      //对parquet1按key去重，取最大的keyExpTime对应的记录，这里使用表join进行去重
      parquet1.registerTempTable("blacklist")
      val grouped = sqlContext.sql("select max(keyExpTime) keyExpTime, key from blacklist group by key")
    grouped.show()
      grouped.registerTempTable("groupedlist")
      val distincted = sqlContext.sql("select a.keyExpTime, a.key, a.value from blacklist a join groupedlist b on a.keyExpTime=b.keyExpTime and a.key=b.key")
    distincted.show()
    distincted.collect().foreach( s => toRedis(jedis,s.get(1).toString,s.get(2).toString,s.get(0).toString.toLong))
  }

  /**
    * 将数据存到redis中，按照key，value的形式
    */

  def toRedis(jedis: JedisCluster,flowId:String,flowScoreStrategyCode:String,failureTime:Long){
    val time = ((failureTime - System.currentTimeMillis()) / 1000).toInt
    if (time > 0) {
      if(!jedis.exists(flowId)){
        //time为过期时间（1小时）
        jedis.setex(flowId, time, flowScoreStrategyCode)
      }
    }
  }
}
