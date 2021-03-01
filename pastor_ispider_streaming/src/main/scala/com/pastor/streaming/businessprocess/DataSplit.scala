package com.pastor.streaming.businessprocess

import java.util.regex.Pattern

import com.pastor.common.util.decode.{EscapeToolBox, RequestDecoder}
import com.pastor.common.util.jedis.PropertiesUtil
import com.pastor.common.util.string.CsairStringUtils

import scala.collection.immutable.HashMap

object DataSplit {
  def dateSplit(message: String): _ = {
    if(null != message){
      val valueArray = message.split("#CS#")
      val request = if(valueArray.length>1) valueArray(1).split(" ")(1) else ""
      val requestMethod = if (valueArray.length > 2) valueArray(2) else ""
      val contentType = if (valueArray.length > 3) valueArray(3) else ""
      val requestBody = if (valueArray.length > 4) valueArray(4) else ""
      val httpReferrer = if (valueArray.length > 5) valueArray(5) else ""
      val remoteAddr = if (valueArray.length > 6) valueArray(6) else ""
      val httpUserAgent = if (valueArray.length> 7) valueArray(7) else ""
      val timeIso8601 = if (valueArray.length > 8) valueArray(8) else ""
      val serverAddr = if (valueArray.length > 9) valueArray(9) else ""
      val cookiesStr = CsairStringUtils.trimSpacesChars(if (valueArray.length > 10) valueArray(10) else "")
      val cookieMap ={
        var map = new HashMap[String,String]()
        if(!cookiesStr.equals()){
          cookiesStr.split(";").foreach(value =>{
            val kvArray = value.split("=")
            if(kvArray.length > 1){
              try {
                  val chPattern = Pattern.compile("u([0-9a-fA-F]{4})")
                  val chMatcher = chPattern.matcher(kvArray(1))
                  var isUnicode = false
                  while (chMatcher.find()){
                    isUnicode= true
                  }
                  if (isUnicode) {
                    map += (kvArray(0) -> EscapeToolBox.unescape(kvArray(1)))
                  } else {
                    map += (kvArray(0) -> RequestDecoder.decodePostRequest(kvArray(1)))
                  }
              } catch {
                case e: Exception => e.printStackTrace()
              }
            }
          })
        }
        map
      }
      val cookieKey_JSESSIONID = PropertiesUtil.getStringByKey("cookie.JSESSIONID.key","cookieConfig.properties")
      val cookieKey_userId4logCookie = PropertiesUtil.getStringByKey("cookie.userId.key","cookieConfig.properties")
      val cookieValue_JSESSIONID = cookieMap.getOrElse(cookieKey_JSESSIONID, "NULL")
      val cookieValue_USERID = cookieMap.getOrElse(cookieKey_userId4logCookie, "NULL")
      (request,requestMethod,contentType,requestBody,httpReferrer,remoteAddr,httpUserAgent,timeIso8601,serverAddr,cookiesStr,cookieValue_JSESSIONID,cookieValue_USERID)
    }
  }
}
