package com.pastor.common.util.database

import java.util

import com.pastor.streaming.constants.{BehaviorTypeEnum, FlightTypeEnum}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/** **************************************************************************
 * NAME:                ScalikeDBUtils                                      *
 * PURPOSE:            Scalike查询数据库                                      *
 * ***************************************************************************
 * REVISIONS:                                                               *
 * VER          DATE            AUTHOP               DESCRIPTION            *
 * *******   ************    **************         ***************          *
 * 1.0       2021-02-23        zhaoyang           新增ScalikeDBUtils工具类    *
 * ***************************************************************************
 * explain:                                                                *
 *      TODO... ScalikeDBUtils，用于操作MySQL数据库，读取清洗规则。               *
 * ***************************************************************************/
object ScalikeDBUtils {

  /**
   * 读取MySQL数据预处理清洗规则函数
   */
  def queryDBMatch(): List[String] = {
    val sql = "select value from nh_filter_rule"
    val field = "value"
    queryDB(sql, field)
  }

  /**
   * 读取MySQL中航班数据查询操作
   * 航班类型: 0国内 1国外
   * 操作类型: 0查询 1预订
   * 0 0 -> 国内查询
   * 0 1 -> 国内预定
   * 1 0 -> 国际查询
   * 1 1 -> 国际预定
   */
  def queryRuleMap()= {
    //0 0 -> 国内查询
    val NCsql = s"SELECT expression FROM nh_classify_rule WHERE flight_type = ${FlightTypeEnum.National.id} AND operation_type = ${BehaviorTypeEnum.Query.id}"
    //0 1 -> 国内预定
    val NYsql = s"SELECT expression FROM nh_classify_rule WHERE flight_type = ${FlightTypeEnum.National.id} AND operation_type = ${BehaviorTypeEnum.Book.id}"
    //1 0 -> 国际查询
    val WCsql = s"SELECT expression FROM nh_classify_rule WHERE flight_type = ${FlightTypeEnum.International.id} AND operation_type = ${BehaviorTypeEnum.Query.id}"
    //1 1 -> 国际预定
    val WYsql = s"SELECT expression FROM nh_classify_rule WHERE flight_type = ${FlightTypeEnum.International.id} AND operation_type = ${BehaviorTypeEnum.Book.id}"
    val field = "expression"

    val queryMap = new util.HashMap[String, List[String]]()
    queryMap.put("nationalQuery",queryDB(NCsql,field))
    queryMap.put("nationalQuery",queryDB(NYsql,field))
    queryMap.put("nationalQuery",queryDB(WCsql,field))
    queryMap.put("internationalBook",queryDB(WYsql,field))
    queryMap
  }

  /**
   * 执行数据查询的底层函数
   */
  private def queryDB(sql: String, field: String): List[String] = {

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




  }
}
