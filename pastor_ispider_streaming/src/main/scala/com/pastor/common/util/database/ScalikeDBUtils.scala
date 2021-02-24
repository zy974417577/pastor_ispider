package com.pastor.common.util.database

import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/****************************************************************************
 * NAME:                ScalikeDBUtils                                      *
 * PURPOSE:            Scalike查询数据库                                      *
 ****************************************************************************
 * REVISIONS:                                                               *
 * VER          DATE            AUTHOP               DESCRIPTION            *
 ********   ************    **************         ***************          *
 * 1.0       2021-02-23        zhaoyang           新增ScalikeDBUtils工具类    *
 ****************************************************************************
 *  explain:                                                                *
 *      TODO... IpListCount公共类，用于链路统计                                 *
 ****************************************************************************/
object ScalikeDBUtils {

  def queryDB(sql:String,field:String): Unit ={

    DBs.setupAll()

    DB.readOnly { implicit  session =>
      SQL(sql)
        .map(rs=>{
          rs.string(field)
        })
    }
  }

}
