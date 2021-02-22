package com.pastor.common.util.database

import scala.collection.mutable.ArrayBuffer

/**
  * 工具类的作用：通过sql和value读取数据库的某一个字段
  *  
  */
object QueryDB {
  def queryData(sql: String, field: String): ArrayBuffer[String] = {
    //创建ab，用来封装数据
    val arr = new ArrayBuffer[String]()
    //获取连接
    val conn = c3p0Util.getConnection
    //执行sql语句
    val ps = conn.prepareStatement(sql)
    val rs = ps.executeQuery()
    //封装数据
    while (rs.next()){
      arr.+=(rs.getString(field))
    }
    c3p0Util.close(conn,ps,rs)
    //返回结果
    arr
  }
}
