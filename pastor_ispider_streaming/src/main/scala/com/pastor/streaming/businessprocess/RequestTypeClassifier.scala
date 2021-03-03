package com.pastor.streaming.businessprocess

import java.util

import com.pastor.common.bean.RequestType
import com.pastor.streaming.constants.{BehaviorTypeEnum, FlightTypeEnum}

/*****************************************************************************
 * NAME:                RequestTypeClassifier                                *
 * PURPOSE:            RequestTypeClassifier判断数据是否国内国外查询或者预定       *
 *****************************************************************************
 * REVISIONS:                                                                *
 * VER          DATE            AUTHOP               DESCRIPTION             *
 * *******   ************    **************         ***************          *
 * 1.0       2021-03-03        zhaoyang      新增RequestTypeClassifier工具类   *
 *****************************************************************************
 * explain:                                                                  *
 *      TODO... RequestTypeClassifier，用于预处理，匹配数据是否为国内或者是国际查询。 *
 *****************************************************************************/
object RequestTypeClassifier {
  /**
   * 用于预处理，匹配数据是否为国内或者是国际查询
   * @param request 数据的查询url
   * @param ruleMap 封装了国内查询、国内预定、国际查询、国际预定的匹配规则
   * @return RequestType样例类,用于标记
   */
  def classifyByRequest(request: String, ruleMap: util.HashMap[String, List[String]]): RequestType = {
    var requestType: RequestType = null
    var flag = true
    //国内查询
    val nationalQuerys = ruleMap.getOrDefault("nationalQuery", null)
    //国内预定
    val nationalBooks = ruleMap.getOrDefault("nationalBook", null)
    //国际查询
    val internationalQuerys = ruleMap.getOrDefault("internationalQuery", null)
    //国际预定
    val internationalBooks = ruleMap.getOrDefault("internationalBook", null)
    //封装国内查询
    nationalBooks.foreach(mess=>{
      if(request.matches(mess) && flag){
        requestType = RequestType(FlightTypeEnum.National,BehaviorTypeEnum.Query)
        flag = false
      }
    })
    //封装国内预定
    internationalQuerys.foreach(mess=>{
      if(request.matches(mess) && flag){
        requestType = RequestType(FlightTypeEnum.National,BehaviorTypeEnum.Book)
        flag = false
      }
    })
    //封装国际查询
    nationalQuerys.foreach(mess=>{
      if(request.matches(mess) && flag){
        requestType = RequestType(FlightTypeEnum.International,BehaviorTypeEnum.Query)
        flag = false
      }
    })
    //封装国际预定
    internationalBooks.foreach(mess=>{
      if(request.matches(mess) && flag){
        requestType = RequestType(FlightTypeEnum.International,BehaviorTypeEnum.Book)
        flag = false
      }
    })
    //其他情况
    if(flag){
      requestType = RequestType(FlightTypeEnum.Other,BehaviorTypeEnum.Other)
    }
    requestType
  }
}
