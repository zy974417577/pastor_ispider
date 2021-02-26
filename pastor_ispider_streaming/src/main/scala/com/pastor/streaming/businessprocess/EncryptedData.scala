package com.pastor.streaming.businessprocess

import java.util.regex.Pattern

import com.pastor.common.util.decode.MD5

/** **************************************************************************
 * NAME:                    EncryptedData                                   *
 * PURPOSE:               EncryptedData使用MD5加密手机号。                     *
 * ***************************************************************************
 * REVISIONS:                                                               *
 * VER          DATE            AUTHOP               DESCRIPTION            *
 * *******   ************    **************         ***************          *
 * 1.0       2021-02-26        zhaoyang           新增EncryptedData类        *
 * ***************************************************************************
 * explain:                                                                *
 *      TODO... EncryptedDatar类，用于对手机号身份证进行脱敏操作采用MD5的          *
 * 方式进行加密                                                         *
 * ***************************************************************************/
object EncryptedData {
  /**
   * 身份证加密
   *
   * @param messageRDD
   * @return
   */
  def encryptedID(messageRDD: String): String = {

    var newMessage = messageRDD
    val md5 = new MD5
    val idPattern = Pattern.compile("(\\d{18})|(\\d{17}(\\d|X|x))|(\\d{15})")

    val idMatcher = idPattern.matcher(messageRDD)

    while (idMatcher.find()) {
      //获取身份证前一个字符
      val lowIndex = messageRDD.indexOf(idMatcher.group()) - 1
      //获取身份证后一个字符位置
      val highIndex = lowIndex + idMatcher.group().length + 1
      //获取身份证前一个字符
      val strIndex = messageRDD.charAt(lowIndex).toString
      //判断身份证前一个字符是否为数字
      if (!strIndex.matches("^[0-9]$")) {
        //判断手机号是否小于数据总长度
        if (highIndex < messageRDD.length) {
          //判断手机号最后一个字符
          val strHigh = messageRDD.charAt(highIndex).toString
          if (!strHigh.matches("^[0-9]$")) {
            newMessage = newMessage.replace(idMatcher.group(), md5.getMD5ofStr(idMatcher.group()))
          }
        } else {
          newMessage = newMessage.replace(idMatcher.group(), md5.getMD5ofStr(idMatcher.group()))
        }
      }
    }
    newMessage
  }

  /**
   * 手机号加密
   *
   * @param messageRDD
   * @return
   */
  def encryptedPhone(messageRDD: String): String = {
    val md5 = new MD5
    var newMessage = messageRDD
    // 手机号匹配正则表达式
    val phonePattern = Pattern.compile("((13[0-9])|(14[5|7])|(15([0-3]|[5-9]))|(17[0-9])|(18[0,5-9]))\\d{8}")
    val matcher = phonePattern.matcher(messageRDD)
    while (matcher.find()) {
      //获取手机号前一个字符
      val lowIndex = messageRDD.indexOf(matcher.group()) - 1
      //获取手机号后一个字符位置
      val highIndex = lowIndex + 12
      //获取手机号前一个字符
      val strIndex = messageRDD.charAt(lowIndex).toString
      //判断手机号前一个字符是否为数字
      if (!strIndex.matches("^[0-9]$")) {
        //判断手机号是否小于数据总长度
        if (highIndex < messageRDD.length) {
          //判断手机号最后一个字符
          val strHigh = messageRDD.charAt(highIndex).toString
          if (!strHigh.matches("^[0-9]$")) {
            newMessage = newMessage.replace(matcher.group(), md5.getMD5ofStr(matcher.group()))
          }
        } else {
          newMessage = newMessage.replace(matcher.group(), md5.getMD5ofStr(matcher.group()))
        }
      }
    }
    newMessage
  }


}
