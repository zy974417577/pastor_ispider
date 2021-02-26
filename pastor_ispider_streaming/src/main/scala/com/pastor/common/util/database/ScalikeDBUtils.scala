package com.pastor.common.util.database

import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/****************************************************************************
 * NAME:                ScalikeDBUtils                                      *
 * PURPOSE:            Scalike查询数据库                                      *
 ****************************************************************************
 * REVISIONS:                                                               *
 * VER          DATE            AUTHOP               DESCRIPTION            *
 ********   ************    **************         ***************          *
 * 1.0       2021-02-23        zhaoyang           新增ScalikeDBUtils工具类    *
 ****************************************************************************
 *  explain:                                                                *
 *      TODO... ScalikeDBUtils，用于操作MySQL数据库，读取清洗规则。               *
 ****************************************************************************/
object ScalikeDBUtils {

  def queryDB(sql:String,field:String = "value"): List[String] ={

    DBs.setupAll()

    val ruleArray: List[String] = DB.readOnly { implicit session =>
      SQL(sql)
        .map(rs => {
          rs.string(field)
        }).list().apply()
    }
    ruleArray
  }

  def main(args: Array[String]): Unit = {

    val sql = "select id,name,age from student"
    val field = "name"

    queryDB(sql,field)
  }
}
