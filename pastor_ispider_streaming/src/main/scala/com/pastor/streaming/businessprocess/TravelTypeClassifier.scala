package com.pastor.streaming.businessprocess

import com.pastor.common.bean.RequestType
import com.pastor.common.util.xml.xmlUtil
import com.pastor.streaming.constants.TravelTypeEnum.TravelTypeEnum
import com.pastor.streaming.constants.{BehaviorTypeEnum, TravelTypeEnum}

/** **************************************************************************
 * NAME:                    TravelTypeClassifier                             *
 * PURPOSE:               TravelTypeClassifier标记是否往返、单程                *
 * ***************************************************************************
 * REVISIONS:                                                                *
 * VER          DATE            AUTHOP               DESCRIPTION             *
 * *******   ************    **************         ***************          *
 * 1.0       2021-03-07        zhaoyang           新增TravelTypeClassifier    *
 * ***************************************************************************
 * explain:                                                                  *
 * 飞行类查询：单程、往返；测试数据只有查询没有生产预定数据暂时不做预定的处理业务          *
 * ***************************************************************************/
object TravelTypeClassifier {
  /**
   * 判断是单程或者往返查询记录
   * http_refer ：
   * http://b2c.csair.com/B2C40/modules/bookingnew/main/flightSelectDirect.html?
   * t=S&
   * c1=CAN&
   * c2=WUH&
   * d1=2018-01-28&
   * at=1&
   * ct=0&
   * it=0
   *
   * @param requestType   url
   * @param httpReferrer  查询
   * @param requestBody   预订
   */
  def classifyByRefererAndRequestBody(requestType: RequestType, httpReferrer: String, requestBody: String) = {
    //定义日期url
    val regex = "^(\\d{4})-(0\\d{1}|1[0-2])-(0\\d{1}|[12]\\d{1}|3[01])$"
    var travelTypeEnum: TravelTypeEnum = null
    //定义计数器
    var count = 0
    //判断是否查询类型
    if (requestType.behaviorType.id == BehaviorTypeEnum.Query.id) {
      //按照'?'切分httpReferrer
      if (httpReferrer.contains("?") && httpReferrer.split("\\?").length > 1) {
        val httpArray = httpReferrer.split("\\?")(1).split("&")
        if (httpArray.length > 1) {
          //遍历循环
          httpArray.foreach(mess => {
            //按照=继续切分
            mess.split("=").foreach(x=>{
              //判断是否包含日期的url，包含计数器+1
              if (x.matches(regex)) {
                count = count + 1
              }
            })
          })
        }
      }
    }

    //操作类型为预定：无法拿到订单数据，暂时不做
    if (BehaviorTypeEnum.Book == requestType.behaviorType) {
      travelTypeEnum = xmlUtil.getTravelType(requestBody)
    }

    //判断count
    count match {
      case 1 => travelTypeEnum = TravelTypeEnum.OneWay
      case 2 => travelTypeEnum = TravelTypeEnum.RoundTrip
      case _ => travelTypeEnum = TravelTypeEnum.Unknown
    }
    travelTypeEnum
  }
}
