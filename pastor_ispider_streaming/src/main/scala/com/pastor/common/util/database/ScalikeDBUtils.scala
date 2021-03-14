package com.pastor.common.util.database

import java.util

import com.pastor.common.bean.AnalyzeRule
import com.pastor.streaming.constants.{BehaviorTypeEnum, FlightTypeEnum}
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/*****************************************************************************
 * NAME:                ScalikeDBUtils                                       *
 * PURPOSE:            Scalike查询数据库                                       *
 * ***************************************************************************
 * REVISIONS:                                                                *
 * VER          DATE            AUTHOP               DESCRIPTION             *
 * *******   ************    **************         ***************          *
 * 1.0       2021-02-23        zhaoyang           新增ScalikeDBUtils工具类 。  *
 * 1.1       2021-03-07        zhaoyang           新增queryRule方法           *
 * 1.2       2021-03-14        zhaoyang           新增getBlackIpDB方法        *
 * ***************************************************************************
 * explain:                                                                 *
 *      TODO... ScalikeDBUtils，用于操作MySQL数据库，读取清洗规则。               *
 * ***************************************************************************/
object ScalikeDBUtils {

  /**
   * 查询数据库中黑名单ip数据
   * @return 返回数据中所有黑名单数据
   */
  def getBlackIpDB() = {

    //数据查询的语句
    val sql ="select ip_name from nh_ip_blacklist"
    //接数据字段
    val field="ip_name"
    queryDB(sql,field)
  }

  /**
   * 读取MySQL的航班规则
   *
   * @param behaviorType 数据库中类型 0 查询 1 预订
   */
  def queryRule(behaviorType: Int) = {

    val sql: String = s"select * from analyzerule where behavior_type =${behaviorType}"

    queryDB(sql)
  }


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
  def queryRuleMap() = {
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
    queryMap.put("nationalQuery", queryDB(NCsql, field))
    queryMap.put("nationalBook", queryDB(NYsql, field))
    queryMap.put("internationalQuery", queryDB(WCsql, field))
    queryMap.put("internationalBook", queryDB(WYsql, field))
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

  /**
   *
   * @param sql
   * @return
   */
  private def queryDB(sql: String) = {

    DBs.setupAll()
    val analyzeRule = new AnalyzeRule()
    
    val ruleArray = DB.readOnly { implicit session =>
      SQL(sql)
        .map(rs => {
          analyzeRule.id = rs.string("id")
          analyzeRule.flightType = rs.string("flight_type").toInt
          analyzeRule.BehaviorType = rs.string("behavior_type").toInt
          analyzeRule.requestMatchExpression = rs.string("requestMatchExpression")
          analyzeRule.requestMethod = rs.string("requestMethod")
          analyzeRule.isNormalGet = rs.string("isNormalGet").toBoolean
          analyzeRule.isNormalForm = rs.string("isNormalForm").toBoolean
          analyzeRule.isApplicationJson = rs.string("isApplicationJson").toBoolean
          analyzeRule.isTextXml = rs.string("isTextXml").toBoolean
          analyzeRule.isJson = rs.string("isJson").toBoolean
          analyzeRule.isXML = rs.string("isXML").toBoolean
          analyzeRule.formDataField = rs.string("formDataField")
          analyzeRule.book_bookUserId = rs.string("book_bookUserId")
          analyzeRule.book_bookUnUserId = rs.string("book_bookUnUserId")
          analyzeRule.book_psgName = rs.string("book_psgName")
          analyzeRule.book_psgType = rs.string("book_psgType")
          analyzeRule.book_idType = rs.string("book_idType")
          analyzeRule.book_idCard = rs.string("book_idCard")
          analyzeRule.book_contractName = rs.string("book_contractName")
          analyzeRule.book_contractPhone = rs.string("book_contractPhone")
          analyzeRule.book_depCity = rs.string("book_depCity")
          analyzeRule.book_arrCity = rs.string("book_arrCity")
          analyzeRule.book_flightDate = rs.string("book_flightDate")
          analyzeRule.book_cabin = rs.string("book_cabin")
          analyzeRule.book_flightNo = rs.string("book_flightNo")
          analyzeRule.query_depCity = rs.string("query_depCity")
          analyzeRule.query_arrCity = rs.string("query_arrCity")
          analyzeRule.query_flightDate = rs.string("query_flightDate")
          analyzeRule.query_adultNum = rs.string("query_adultNum")
          analyzeRule.query_childNum = rs.string("query_childNum")
          analyzeRule.query_infantNum = rs.string("query_infantNum")
          analyzeRule.query_country = rs.string("query_country")
          analyzeRule.query_travelType = rs.string("query_travelType")
          analyzeRule.book_psgFirName = rs.string("book_psgFirName")
          analyzeRule
        }).list().apply()
    }
    ruleArray
  }
}
