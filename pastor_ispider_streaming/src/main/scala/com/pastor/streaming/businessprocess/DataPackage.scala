package com.pastor.streaming.businessprocess

import com.pastor.common.bean.{BookRequestData, CoreRequestParams, ProcessedData, QueryRequestData, RequestType}
import com.pastor.streaming.constants.TravelTypeEnum.TravelTypeEnum

/** **************************************************************************
 * NAME:                  DataPackage                                       *
 * PURPOSE:           封装结构化数据ProcessedData                              *
 * ***************************************************************************
 * REVISIONS:                                                               *
 * VER          DATE            AUTHOP               DESCRIPTION            *
 * *******   ************    **************         ***************          *
 * 1.0       2021-03-21        zhaoyang           新增加DataPackage          *
 * ***************************************************************************
 * explain:                                                                 *
 *      TODO... 用户封装结构化数据，预处理阶段最后封装成ProcessedData数据类型        *
 * ***************************************************************************/
object DataPackage {

  /**
   * 封装ProcessedData，根据queryRequestData、bookRequestData来解析：始发地、目的地、起飞时间
   * 封装成CoreRequestParams类型，然后返回ProcessedData。
   * @param sourceData：请求原始数据
   * @param requestMethod：请求方法
   * @param request：请求路径
   * @param remoteAddr：客户端 ip
   * @param httpUserAgent：代理
   * @param timeIso8601：时间
   * @param serverAddr：请求的服务器地址
   * @param highFrqIPGroup：此次请求中的 ip 地址是否命中高频 ip
   * @param requestType：请求类型
   * @param travelType：往返类型
   * @param cookieValue_JSESSIONID： cookie 中的 jessionid
   * @param cookieValue_USERID： cookie 中的 userid
   * @param queryRequestData：查询请求的 form 数据
   * @param bookRequestData: 预定请求的from
   * @return 返回ProcessedData
   */
  def dataPackage(sourceData: String, requestMethod: String, request: String, remoteAddr: String,
                  httpUserAgent: String, timeIso8601: String, serverAddr: String,
                  highFrqIPGroup: Boolean, requestType: RequestType, travelType: TravelTypeEnum,
                  cookieValue_JSESSIONID: String, cookieValue_USERID: String,
                  queryRequestData: Option[QueryRequestData],
                  bookRequestData: Option[BookRequestData], httpReferrer: String): ProcessedData = {
    //始发地
    var depCity = ""
    //目的地
    var arrCity = ""
    //起飞时间
    var flightDate = ""
    //查询数据中获取始发地、目的地、起飞时间
    queryRequestData match {
      case Some(value) => {
        depCity = value.depCity
        arrCity = value.arrCity
        flightDate = value.flightDate
      }
      case None =>
    }
    if(null == flightDate && null == arrCity && null == depCity){
      bookRequestData match {
        case Some(value) => {
          depCity = value.depCity.toString()
          arrCity = value.arrCity.toString()
          flightDate = value.flightDate.toString()
        }
        case None =>
      }
    }
    ProcessedData(sourceData, requestMethod, request,
      remoteAddr, httpUserAgent, timeIso8601,
      serverAddr, highFrqIPGroup,
      requestType, travelType, CoreRequestParams(flightDate,depCity,arrCity),
      cookieValue_JSESSIONID, cookieValue_USERID,
      queryRequestData, bookRequestData,
      httpReferrer)
  }

}
