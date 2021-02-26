package com.pastor.streaming.businessprocess

/****************************************************************************
 * NAME:                    URLFilter                                       *
 * PURPOSE:               URLFilter过滤URL                                   *
 ****************************************************************************
 * REVISIONS:                                                               *
 * VER          DATE            AUTHOP               DESCRIPTION            *
 ********   ************    **************         ***************          *
 * 1.0       2021-02-24        zhaoyang           新增URLFilter类            *
 ****************************************************************************
 *  explain:                                                                *
 *      TODO... URLFilter类，用于过滤数据，过滤规则从数据库中读取                  *
 ****************************************************************************/
object URLFilter {
  /**
   * 用于过滤不配数据库中的规则的数据
   * @param message rdd的内容
   * @param list  数据库中的规则
   * @return 返回符合规则的数据
   */
  def filterURL(message:String, list: List[String]):Boolean = {
    var flag = true;
    val valueArray = message.split("#CS#")
    if(valueArray.length > 1){
      list.foreach(x=>{
//        println(valueArray(1)+" "+ x)
        if(valueArray(1).matches(x)) flag = false else flag
      })
    }
    flag
  }
}
