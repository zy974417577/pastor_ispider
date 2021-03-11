package com.pastor.streaming.businessprocess

import com.pastor.common.bean.{AnalyzeRule, BookRequestData, RequestType}
import com.pastor.common.util.decode.{MD5, RequestDecoder}
import com.pastor.common.util.json.JsonParser
import com.pastor.common.util.xml.xmlUtil
import com.pastor.streaming.constants.FlightTypeEnum
import com.pastor.streaming.constants.TravelTypeEnum.TravelTypeEnum
/****************************************************************************
 * NAME:                AnalyzeBookRequest                                  *
 * PURPOSE:           解析规则解析预定中的 body 数据                            *
 ****************************************************************************
 * REVISIONS:                                                               *
 * VER          DATE            AUTHOP               DESCRIPTION            *
 ********   ************    **************         ***************          *
 * 1.0       2021-03-11        zhaoyang           新增加AnalyzeBookRequest   *
 ****************************************************************************
 *  explain:                                                                *
 *      TODO... 用于解析数据中body的预定数据，解析出出发地、目的地、乘坐人等信息       *
 ****************************************************************************/

object AnalyzeBookRequest {
  /**
   * 将数据进行分析，得出规则
    *
   * @param requestTypeLabel
   * @param requestMethod
   * @param contentType
   * @param request
   * @param requestBody
   * @param travelType
   * @return
   */
  def analyzeBookRequest(requestTypeLabel: RequestType, requestMethod: String, contentType: String, request: String, requestBody: String, travelType: TravelTypeEnum, analyzeRules: List[AnalyzeRule]): Option[BookRequestData] = {
    var resultData: BookRequestData = new BookRequestData
    //循环规则，匹配出符合数据的规则
    val matchedRules = analyzeRules.filter { rule =>
      //匹配请求方式和请求类型 - 预定
      if (rule.requestMethod.equalsIgnoreCase(requestMethod) && rule.BehaviorType == requestTypeLabel.behaviorType.id)
        true
      else
        false
    }.filter { rule =>
      request.matches(rule.requestMatchExpression)
    }
    //匹配出规则，解析
    if (matchedRules.size > 0) {
      val matchedRule = matchedRules.last
      //国内
      val nationalId = FlightTypeEnum.National.id
      //国际
      val internationalId = FlightTypeEnum.International.id
      //xml类型数据
      if (contentType.equalsIgnoreCase("text/xml") && matchedRule.isTextXml && matchedRule.isXML && requestBody.nonEmpty) {
        resultData = xmlUtil.parseXML(requestBody, matchedRule)
      }
      //json类型数据
      else if (contentType.equalsIgnoreCase("application/json") && !matchedRule.isNormalForm && matchedRule.isApplicationJson && matchedRule.isJson && requestBody.nonEmpty) {
        resultData = JsonParser.parseJsonToBean(requestBody, matchedRule)
      }
      //json类型数据
      else if (!matchedRule.isNormalForm && matchedRule.isJson && !matchedRule.isApplicationJson && matchedRule.formDataField.nonEmpty) {
        val paramMap = scala.collection.mutable.Map[String, String]()
        val params = requestBody.split("&")
        for (param <- params) {
          val keyAndValue = param.split("=")
          if (keyAndValue.length > 1) {
            paramMap += (keyAndValue(0) -> RequestDecoder.decodePostRequest(keyAndValue(1)))

          }
        }
        var json = paramMap.getOrElse(matchedRule.formDataField, "")
        if ("[".equals(json.charAt(0).toString()) && "]".equals(json.charAt(json.length() - 1).toString())) {
          json = json.substring(1, json.length() - 1)
        }
        if (json.trim.nonEmpty) {
          resultData = JsonParser.parseJsonToBean(json, matchedRule)
        }
      }

      matchedRule.flightType match {
        //国内
        case `nationalId` => {
          resultData.flightType = FlightTypeEnum.National.id
          resultData.travelType = travelType.id
        }
        //国际
        case `internationalId` => {
          resultData.flightType = FlightTypeEnum.International.id
          resultData.travelType = travelType.id
        }
        case _ => None
      }
    }
    //resultData
    if (resultData != null) {
      //加密手机号，证件号。
      val md5 = new MD5()
      resultData.contractPhone = md5.getMD5ofStr(resultData.contractPhone)
      val idCardEncrypted = scala.collection.mutable.ListBuffer[String]()

      for (record <- resultData.idCard) {
        idCardEncrypted.append(md5.getMD5ofStr(record))
      }

      resultData.idCard = idCardEncrypted

      Some(resultData)
    } else {
      None
    }
  }
}
