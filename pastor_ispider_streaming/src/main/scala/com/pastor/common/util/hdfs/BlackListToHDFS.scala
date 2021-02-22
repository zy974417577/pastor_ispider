package com.pastor.common.util.hdfs

import com.pastor.common.util.jedis.PropertiesUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}


/**
 * 黑名单保存到HDFS服务
 */
object BlackListToHDFS {
  /**
    * 保存黑名单到HDFS
    *
    * @param antiBlackListRDD：传入黑名单RDD
    * @param sqlContext：传入入sqlContext创建DataFrame
    */
  def saveAntiBlackList(antiBlackListRDD: RDD[Row], sqlContext: SQLContext) ={

    //构建DataFrame
    val tableCols = List("keyExpTime","key","value")
    val schemaString=tableCols.mkString("-")
    val schema = StructType(schemaString.split("-").map(fieldName => StructField(fieldName, StringType, true)))
    val dataFrame: DataFrame = sqlContext.createDataFrame(antiBlackListRDD,schema)
    val path: String = PropertiesUtil.getStringByKey("blackListPath","HDFSPathConfig.properties")
    HdfsSaveUtil.save(dataFrame,null,path)
  }

  def saveAntiOcpBlackList(): Unit ={


  }

}
