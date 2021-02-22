package com.pastor.common.util.kafka


import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.zookeeper.data.Stat

 
object KafkaOffsetUtil {
  val logger = Logger.getLogger("KafkaOffsetUtil")

  /**
    * 从zookeeper指定的path路径中去读数据
    * @param client zookeeperClient
    * @param path 路径
    * @return option 对象
    */
  def readDataMaybeNull(client: ZkClient, path: String): (Option[String], Stat) = {
    val stat: Stat = new Stat()
    val dataAndStat = try {
      (Some(client.readData(path, stat)), stat)
    } catch {
      case e: ZkNoNodeException =>
        (None, stat)
      case e2: Throwable => throw e2
    }
    dataAndStat
  }

  /**
    * 将数据写入zookeeper中指定的路径中，如果该路径存在父目录且父目录不存在则先创建父目录
    * @param client zookeeperClient
    * @param path 路径
    * @param data 数据
    */
  def updatePersistentPath(client: ZkClient, path: String, data: String) = {
    try {
      client.writeData(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        try {
          client.createPersistent(path, data)
        } catch {
          case e: ZkNodeExistsException =>
            client.writeData(path, data)
          case e2: Throwable => throw e2
        }
      }
      case e2: Throwable => throw e2
    }
  }

  /**
    * 创建父目录
    * @param client zookeeperClient
    * @param path 路径
    */
  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }

  /*
   Read the previously saved offsets from Zookeeper
    */
  /**
    * 取kafka中的偏移量
    * @param zkClient zookeeperClient
    * @param zkHosts zookeeperhosts节点
    * @param zkPath  zookeeper路径
    * @param topic  kafka中的topic
    * @return  Option对象
    */
  def readOffsets(zkClient: ZkClient,
                  zkHosts: String,
                  zkPath: String,
                  topic: String): Option[Map[TopicPartition, Long]] = {
    logger.info("Reading offsets from Zookeeper")
    val stopwatch = new Stopwatch()
    val (offsetsRangesStrOpt, _) = readDataMaybeNull(zkClient, zkPath)
    offsetsRangesStrOpt match {
      case Some(offsetsRangesStr) =>
        logger.info(s"Read offset ranges: ${offsetsRangesStr}")
        val offsets = offsetsRangesStr.split(",")
          .map(s => s.split(":"))
          .map { case Array(partitionStr, offsetStr) => new TopicPartition(topic, partitionStr.toInt) -> offsetStr.toLong }
          .toMap
        logger.info("Done reading offsets from Zookeeper. Took " + stopwatch)
        Some(offsets)
      case None =>
        logger.info("No offsets found in Zookeeper. Took " + stopwatch)
        None
    }
  }

  /**
    * 将offest偏移量保存到zookeeper中，并打上日志
    * @param zkClient zookeeperClient
    * @param zkHosts  zookeeperhosts节点
    * @param zkPath  zookeeper路径
    * @param rdd 偏移量的rdd
    */
  def saveOffsets(zkClient: ZkClient,
                  zkHosts: String,
                  zkPath: String,
                  rdd: RDD[_]): Unit = {
    logger.info("Saving offsets to Zookeeper")
    val stopwatch = new Stopwatch()
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    offsetsRanges.foreach(offsetRange => logger.debug(s"Using ${offsetRange}"))
    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
    logger.info("Writing offsets to Zookeeper zkClient=" + zkClient + "  zkHosts=" + zkHosts + "zkPath=" + zkPath + "  offsetsRangesStr:" + offsetsRangesStr)
    updatePersistentPath(zkClient, zkPath, offsetsRangesStr)
    logger.info("Done updating offsets in Zookeeper. Took " + stopwatch)
  }

  /**
    * 过程时间
    */
  class Stopwatch {
    private val start = System.currentTimeMillis()
    override def toString() = (System.currentTimeMillis() - start) + " ms"
  }
}