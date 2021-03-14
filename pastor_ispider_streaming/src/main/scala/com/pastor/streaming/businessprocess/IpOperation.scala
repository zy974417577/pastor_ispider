package com.pastor.streaming.businessprocess

/*****************************************************************************
 * NAME:                IpOperation                                          *
 * PURPOSE:            IpOperation判断是否为黑名单标识                           *
 * ***************************************************************************
 * REVISIONS:                                                                *
 * VER          DATE            AUTHOP               DESCRIPTION             *
 * *******   ************    **************         ***************          *
 * 1.0       2021-03-14        zhaoyang           新增IpOperation工具类        *
 * ***************************************************************************
 * explain:                                                                  *
 *      TODO... IpOperation，用于判断是否为黑名单ip                              *
 * ***************************************************************************/
object IpOperation {
  /**
   *  用于过滤当前数据是否为黑名单数据
   * @param remoteAddr 当前数据ip
   * @param value 数据库中黑名ip
   * @return TODO... 如果匹配上返回true,否则返回false
   */
  def isFreIP(remoteAddr: String, value: List[String]) = {
    var flagIP = false
    value.foreach(ip=>{
      if(remoteAddr.equals(ip)){
          flagIP = true
      }
    })
    flagIP
  }
}
