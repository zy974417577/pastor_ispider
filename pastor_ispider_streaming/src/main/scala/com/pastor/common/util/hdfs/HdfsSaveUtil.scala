package com.pastor.common.util.hdfs

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.streaming.Time

/**
 * 通用HDFS数据保存服务
 */
object HdfsSaveUtil {

  /**
   * 基于时间生成文件夹保存DataFrame数据到HDFS
   * @param dataFrame 数据
   * @param time 数据时间
   * @param paths 保存路径
   */
  def save(dataFrame:DataFrame,time:Time,paths:String): Unit ={
    val date  = if(time == null){
      new SimpleDateFormat("yyyy/MM/dd/HH").format(System.currentTimeMillis())
    }else{
      new SimpleDateFormat("yyyy/MM/dd/HH").format(new Date(time.milliseconds))
    }
    val path = paths+date
    dataFrame.write.mode(SaveMode.Append).format("parquet").save(path)
  }

}
