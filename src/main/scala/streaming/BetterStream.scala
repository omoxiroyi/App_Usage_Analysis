package streaming

import java.util.Calendar

import hbase.HbaseBean
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import play.api.libs.json.Json
import streaming.Util._

object BetterStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Analysis")
    val ssc = new StreamingContext(conf, Seconds(50))

    ssc.checkpoint("hdfs://master:8020/checkpoint")

    val input = ssc.socketTextStream("master", 9999)

    val data = input.flatMap(Json.parse(_).asOpt[DataBean])

    /** ********************************************************************************/
    /** *******************************计算应用无关变量 *********************************/
    /** ********************************************************************************/

    //累计五分钟存储一次应用无关变量数据 app irrelevant variables
    val AIV = data //.window(Seconds(10), Seconds(10))

    //将数据按不同日期分类
    val DateSplitAIV = AIV.transform(rdd => rdd.groupBy(_.date))

    DateSplitAIV.foreachRDD { rdd =>
      rdd.foreachPartition { part =>
        implicit val conn: Connection = HbaseBean.getNewConnection
        part.foreach { aiv =>
          val SourceData = HbaseBean.getOneRecord("AIV", aiv._1)

          //----------------------统计品牌量----------------------
          val oldBrandMap = scala.collection.mutable.HashMap[String, Int]()
          Option(SourceData.getFamilyMap("brand".getBytes)).foreach(_ forEach ((k, v) => oldBrandMap += ((new String(k), new String(v).toInt))))
          val oldBrandData = oldBrandMap.toMap
          val newBrandData = aiv._2.map(x => (x.brand, 1)).groupBy(_._1).map { case (k, v) => (k, v.map(_._2).sum) }
          // 合并新老品牌数据 存入Hbase
          HbaseBean.insertBatchRecord("AIV", aiv._1, "brand", oldBrandData./:(newBrandData) {
            case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
          } mapValues (_.toString))

          //----------------------统计机型量----------------------
          val oldModelMap = scala.collection.mutable.HashMap[String, Int]()
          Option(SourceData.getFamilyMap("model".getBytes)).foreach(_ forEach ((k, v) => oldModelMap += ((new String(k), new String(v).toInt))))
          val oldModelData = oldModelMap.toMap
          val newModelData = aiv._2.map(x => (x.model, 1)).groupBy(_._1).map { case (k, v) => (k, v.map(_._2).sum) }
          // 合并新老机型数据 存入Hbase
          HbaseBean.insertBatchRecord("AIV", aiv._1, "model", oldModelData./:(newModelData) {
            case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
          } mapValues (_.toString))

          //----------------------统计系统版本----------------------
          val oldVersionMap = scala.collection.mutable.HashMap[String, Int]()
          Option(SourceData.getFamilyMap("system_version".getBytes)).foreach(_ forEach ((k, v) => oldVersionMap += ((new String(k), new String(v).toInt))))
          val oldVersionData = oldVersionMap.toMap
          val newVersionData = aiv._2.map(x => (x.system_version, 1)).groupBy(_._1).map { case (k, v) => (k, v.map(_._2).sum) }
          HbaseBean.insertBatchRecord("AIV", aiv._1, "system_version", oldVersionData./:(newVersionData) {
            case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
          } mapValues (_.toString))

          //----------------------统计分辨率量----------------------
          val oldResolutionMap = scala.collection.mutable.HashMap[String, Int]()
          Option(SourceData.getFamilyMap("resolution".getBytes)).foreach(_ forEach ((k, v) => oldResolutionMap += ((new String(k), new String(v).toInt))))
          val oldResolutionDate = oldResolutionMap.toMap
          val newResolutionData = aiv._2.map(x => (x.resolution, 1)).groupBy(_._1).map { case (k, v) => (k, v.map(_._2).sum) }
          HbaseBean.insertBatchRecord("AIV", aiv._1, "resolution", oldResolutionDate./:(newResolutionData) {
            case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
          } mapValues (_.toString))
          //----------------------统计联网状态----------------------
          val oldNetMap = scala.collection.mutable.HashMap[String, Int]()
          Option(SourceData.getFamilyMap("net_status".getBytes)).foreach(_ forEach ((k, v) => oldNetMap += ((new String(k), new String(v).toInt))))
          val oldNetData = oldNetMap.toMap
          val newNetData = aiv._2.map(x => (x.net_status, 1)).groupBy(_._1).map { case (k, v) => (k, v.map(_._2).sum) }
          HbaseBean.insertBatchRecord("AIV", aiv._1, "net_status", oldNetData./:(newNetData) {
            case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
          } mapValues (_.toString))
          //---------------------统计运营商类型---------------------
          val oldISPMap = scala.collection.mutable.HashMap[String, Int]()
          Option(SourceData.getFamilyMap("ISP".getBytes)).foreach(_ forEach ((k, v) => oldISPMap += ((new String(k), new String(v).toInt))))
          val oldISPData = oldISPMap.toMap
          val newISPData = aiv._2.map(x => (x.ISP, 1)).groupBy(_._1).map { case (k, v) => (k, v.map(_._2).sum) }
          HbaseBean.insertBatchRecord("AIV", aiv._1, "ISP", oldISPData./:(newISPData) {
            case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
          } mapValues (_.toString))
        }
        conn.close()
      }
    }

    /** ********************************************************************************/
    /** ******************************计算单一app应用量 *********************************/
    /** ********************************************************************************/
    // --------------------------统计app每日启动数--------------------------------

    // 格式化数据为每个用户某一天的打开的某个应用的次数 (k, (v._1, v._2)) => k -> 应用的包名 v._1 -> 日期 v._2 -> 应用打开的次数
    // 对应Json数据格式 [(package_name: xx, date: 2018-3-3, count: 2)]
    val SINGLE_USER_DAYLY_DATA = AIV.flatMap(x => x.apps.map(y => (y.package_name, 1)).groupBy(_._1).map { case (k, v) => (k, x.date -> v.map(_._2).sum) })

    // 合并所有的用户数据 格式化为每个应用多天的打开次数情况 (k, map(x, y)) => k -> 应用的包名 x -> 日期 y -> 应用打开的次数
    // 对应Json数据格式 [(package_name: xx, [date: 2018-3-3, count: 5])]
    val ALL_USER_DAYLY_DATA = SINGLE_USER_DAYLY_DATA.groupByKey.map(x => x._1 -> x._2.groupBy(_._1).map { case (k, v) => (k, v.map(_._2).sum) })

    // 存储数据入Hbase
    ALL_USER_DAYLY_DATA.foreachRDD { rdd =>
      rdd.foreachPartition { part =>
        implicit val conn: Connection = HbaseBean.getNewConnection
        part.foreach { app =>
          // 查询该App的原数据 rowKey为该app的包名
          val oldSource = HbaseBean.getOneRecord("SINGLE_APP", app._1)
          // 创建map存储原数据
          val oldClickMap = scala.collection.mutable.HashMap[String, Int]()
          Option(oldSource.getFamilyMap("click_num".getBytes)).foreach(_ forEach ((k, v) => oldClickMap += ((new String(k), new String(v).toInt))))
          val oldClickData = oldClickMap.toMap
          // 批量插入合并后的数据
          HbaseBean.insertBatchRecord("SINGLE_APP", app._1, "click_num", oldClickData./:(app._2) {
            case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
          } mapValues (_.toString))
        }
      }
    }

    // --------------------------统计app每日启动人数-------------------------------

    // 单个用户每日的app使用状况
    // [package_name: xx, date: 2018-3-10, uid: xx]
    val SINGLE_USER_DAYLY_APP_USAGE = AIV.flatMap(x => x.apps.map(_.package_name).distinct.map(y => (y, x.date, x.user_id)))

    // 单个app每日的用户列表
    // [package_name: xx, [date: 2018-3-10, [uid: xx, uid: zz]]]
    val ALL_USER_DAYLY_APP_USAGE = SINGLE_USER_DAYLY_APP_USAGE.transform(_.groupBy(_._1)).map {
      case (k, v) => k -> v.groupBy(_._2).map {
        case (kk, vv) => kk -> vv.map(_._3).toList.distinct
      }
    }

    // 存储app的每日用户
    ALL_USER_DAYLY_APP_USAGE.foreachRDD { rdd =>
      rdd.foreachPartition { part =>
        implicit val conn: Connection = HbaseBean.getNewConnection
        part.foreach { app =>
          val oldSource = HbaseBean.getOneRecord("SINGLE_APP", app._1)
          val oldUserMap = scala.collection.mutable.HashMap[String, String]()
          Option(oldSource.getFamilyMap("user".getBytes)).foreach(_ forEach ((k, v) => oldUserMap += ((new String(k), new String(v)))))
          val oldUserData = oldUserMap.toMap.mapValues(_.split(",").toList)
          HbaseBean.insertBatchRecord("SINGLE_APP", app._1, "user", oldUserData./:(app._2) {
            case (m, (k, v)) => m + (k -> (v ++ m.getOrElse(k, List[String]())).distinct)
          }.mapValues(_.mkString(",")))
        }
      }
    }


    // --------------------------统计app版本应用量--------------------------------
    // 个人用户的每日应用版本
    // [package_name: xx, date: 2018-3-20, [version: 1.0, count: 5]]
    val SINGLE_APP_VERSION = AIV.flatMap(x => x.apps.map(y => (y.package_name, y.version, 1)).groupBy(_._1).map {
      case (k, v) => (k, x.date, v.groupBy(_._2).map {
        case (kk, vv) => (kk, vv.map(_._3).sum)
      })
    })

    // 单一用户数据群 临时变量
    // [package_name: xx, [version: 1.0, [date: 2018-3-20, count: 5]]]
    val SINGLE_APP_VERSION_TEMP = SINGLE_APP_VERSION.map(x => (x._1, x._3.map(y => (y._1, Seq(x._2 -> y._2)))))

    // 所有用户的数据
    // [package_name:xx [version: 1.0, [date: 2018-3-20, count: 10]]]
    val ALL_APP_VERSION = SINGLE_APP_VERSION_TEMP.transform(_.groupBy(_._1)).map {
      case (k, v) => k -> v.map(_._2).reduce(_./:(_) {
        case (m, (kk, vv)) => m + (kk -> (vv ++ m.getOrElse(kk, Seq[(String, Int)]())))
      }).map(z => (z._1, z._2.groupBy(_._1) map {
        case (kkk, vvv) => (kkk, vvv.map(_._2).sum)
      }))
    }

    // 存储app版本应用量到Hbase
    ALL_APP_VERSION.foreachRDD { rdd =>
      rdd.foreachPartition { part =>
        implicit val conn: Connection = HbaseBean.getNewConnection
        part.foreach { app =>
          app._2.foreach { version =>
            // 无论如何 先存入版本号入SINGLE_APP表中的app_version列族 方便查询版本时索引
            HbaseBean.insertRecord("SINGLE_APP", app._1, "version", version._1, version._1)
            // 查询该App的原数据 rowKey为该app的包名加版本号
            val rowKey = app._1 + "_" + version._1
            val oldSource = HbaseBean.getOneRecord("SINGLE_APP", rowKey)
            val oldClickMap = scala.collection.mutable.HashMap[String, Int]()
            Option(oldSource.getFamilyMap("click_num".getBytes)).foreach(_ forEach ((k, v) => oldClickMap += ((new String(k), new String(v).toInt))))
            val oldClickData = oldClickMap.toMap
            HbaseBean.insertBatchRecord("SINGLE_APP", rowKey, "click_num", oldClickData./:(version._2) {
              case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
            } mapValues (_.toString))
          }
        }
      }
    }

    // --------------------------统计app用户使用时长--------------------------------

    // 单一用户某日各app的使用时长 时间单位 秒
    // [date: 2018-3-10, [package_name: xx, time: 1500s]]
    val SINGLE_USER_APP_USAGE = data.map(x => (x.date, x.apps.groupBy(_.package_name) map { case (k, v) => (k, v.map(y => (y.endTime - y.beginTime) / 1000).sum) }))

    // 根据使用时长计算使用时长分布
    // [date: 2018-3-10, [package_name: xx, time: "0-5分钟"]]  这里用1表示0 - 5分钟 2 表示 5 - 15 分钟
    val SINGLE_USER_APP_USAGE_TRANSFORMED = SINGLE_USER_APP_USAGE.map(x => (x._1, x._2.map {
      case (k, v) if v >= 0 && v < 300 => (k, "1")
      case (k, v) if v >= 300 && v < 900 => (k, "2")
      case (k, v) if v >= 900 && v < 1800 => (k, "3")
      case (k, v) if v >= 1800 && v < 3600 => (k, "4")
      case (k, v) if v >= 3600 && v < 7200 => (k, "5")
      case (k, _) => (k, "6")
    }))

    // 所有app 各天的用户使用时长分布
    // [package_name: xx, [time: "0-5分钟", [2018-3-10, 30人]]]
    val ALL_USER_APP_USAGE_DISTRIBUTION = SINGLE_USER_APP_USAGE_TRANSFORMED.flatMap(x => x._2.map(y => (y._1, y._2, x._1))).transform(_.groupBy(_._1)).map {
      case (k, v) => (k, v.groupBy(_._2).map {
        case (kk, vv) => (kk, vv.map(z => (z._3, 1)).groupBy(_._1).map {
          case (kkk, vvv) => (kkk, vvv.map(_._2).sum)
        })
      })
    }

    // 此Batch各个app 各天使用的时长
    // [package_name: xx, [date: 2018-3-10, time:150000s]]
    val ALL_USER_APP_USAGE_DURATION_SUM = SINGLE_USER_APP_USAGE.flatMap(x => x._2.mapValues(x._1 -> _)).groupByKey.map {
      case (k, v) => k -> v.groupBy(_._1).map {
        case (kk, vv) => kk -> vv.map(_._2).sum
      }
    }

    // 写入单一app总每日使用时长到Hbase
    ALL_USER_APP_USAGE_DURATION_SUM.foreachRDD { rdd =>
      rdd.foreachPartition { part =>
        implicit val conn: Connection = HbaseBean.getNewConnection
        part.foreach { app =>
          val oldSource = HbaseBean.getOneRecord("SINGLE_APP", app._1)
          val oldDurationMap = scala.collection.mutable.HashMap[String, Long]()
          Option(oldSource.getFamilyMap("duration".getBytes)).foreach(_ forEach ((k, v) => oldDurationMap += ((new String(k), new String(v).toLong))))
          val oldDurationData = oldDurationMap.toMap
          HbaseBean.insertBatchRecord("SINGLE_APP", app._1, "duration", oldDurationData./:(app._2) {
            case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0L)))
          }.mapValues(_.toString))
        }
      }
    }

    // 写入单一APP使用时长情况分布到Hbase
    ALL_USER_APP_USAGE_DISTRIBUTION.foreachRDD { rdd =>
      rdd.foreachPartition { part =>
        implicit val conn: Connection = HbaseBean.getNewConnection
        part.foreach { app =>
          val oldSource = HbaseBean.getOneRecord("APP_USAGE", app._1)
          app._2.foreach { time =>
            val oldClickMap = scala.collection.mutable.HashMap[String, Int]()
            Option(oldSource.getFamilyMap(time._1.getBytes)).foreach(_ forEach ((k, v) => oldClickMap += ((new String(k), new String(v).toInt))))
            val oldClickData = oldClickMap.toMap
            HbaseBean.insertBatchRecord("APP_USAGE", app._1, time._1, oldClickData./:(time._2) {
              case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
            } mapValues (_.toString))
          }
        }
      }
    }

    //-------------------------统计app的使用时段-------------------------------

    // 个人使用各种app的日时段 时段分析不分区分日期 累计总值
    // [package_name: xx, [time: 11, count: 4]]
    val SINGLE_USER_APP_USE_DAY_PERIOD = AIV.flatMap(x => x.apps.map(y => (y.package_name, y.beginTime)).groupBy(_._1).map {
      case (k, v) => (k, v.map {
        case (_, vv) => val c = Calendar.getInstance()
          c.setTimeInMillis(vv)
          (c.get(Calendar.HOUR_OF_DAY).toString, 1)
      }.groupBy(_._1).map { case (kkk, vvv) => (kkk, vvv.map(_._2).sum) })
    })

    // 此Batch所有用户的各种app日使用时段
    val ALL_USER_APP_USE_DAY_PERIOD = SINGLE_USER_APP_USE_DAY_PERIOD.reduceByKey(_./:(_) { case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0))) })

    // 个人使用各种app的周时段 时段分析不分区分日期 累计总值
    // [package_name: xx, [time: 11, count: 4]]
    val SINGLE_USER_APP_USE_WEEK_PERIOD = AIV.flatMap(x => x.apps.map(y => (y.package_name, y.beginTime)).groupBy(_._1).map {
      case (k, v) => (k, v.map {
        case (_, vv) => val c = Calendar.getInstance
          c.setTimeInMillis(vv)
          (c.get(Calendar.WEEK_OF_MONTH).toString, 1)
      }.groupBy(_._1).map { case (kkk, vvv) => (kkk, vvv.map(_._2).sum) })
    })

    // 此Batch所有用户的各种app周使用时段
    val ALL_USER_APP_USE_WEEk_PERIOD = SINGLE_USER_APP_USE_WEEK_PERIOD.reduceByKey(_./:(_) { case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0))) })

    // 存储app日使用时段到Hbase
    ALL_USER_APP_USE_DAY_PERIOD.foreachRDD { rdd =>
      rdd.foreachPartition { part =>
        implicit val conn: Connection = HbaseBean.getNewConnection
        part.foreach { app =>
          val oldSource = HbaseBean.getOneRecord("SINGLE_APP", app._1)
          val oldUsageMap = scala.collection.mutable.HashMap[String, Int]()
          Option(oldSource.getFamilyMap("day_period".getBytes)).foreach(_ forEach ((k, v) => oldUsageMap += ((new String(k), new String(v).toInt))))
          val oldUsageDate = oldUsageMap.toMap
          HbaseBean.insertBatchRecord("SINGLE_APP", app._1, "day_period", oldUsageDate./:(app._2) {
            case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
          } mapValues (_.toString))
        }
      }
    }

    // 存储app周使用时段到Hbase
    ALL_USER_APP_USE_WEEk_PERIOD.foreachRDD { rdd =>
      rdd.foreachPartition { part =>
        implicit val conn: Connection = HbaseBean.getNewConnection
        part.foreach { app =>
          val oldSource = HbaseBean.getOneRecord("SINGLE_APP", app._1)
          val oldUsageMap = scala.collection.mutable.HashMap[String, Int]()
          Option(oldSource.getFamilyMap("week_period".getBytes)).foreach(_ forEach ((k, v) => oldUsageMap += ((new String(k), new String(v).toInt))))
          val oldUsageDate = oldUsageMap.toMap
          HbaseBean.insertBatchRecord("SINGLE_APP", app._1, "week_period", oldUsageDate./:(app._2) {
            case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
          } mapValues (_.toString))
        }
      }
    }

    /** ********************************************************************************/
    /** *****************************计算单一用户的应用量 *******************************/
    /** ********************************************************************************/

    // 某一用户每天玩手机的总时长
    val SINGLE_USER_PHONE_USAGE_DURATION = data.map(x => (x.user_id, x.date, x.apps.map(y => y.endTime - y.beginTime).sum))

    // 存入用户每日使用手机时长到Hbase
    SINGLE_USER_PHONE_USAGE_DURATION.foreachRDD { rdd =>
      rdd.foreachPartition { part =>
        implicit val conn: Connection = HbaseBean.getNewConnection
        part.foreach { user =>
          HbaseBean.insertRecord("USER", user._1, "usage_duration", user._2, user._3.toString)
        }
      }
    }

    // 某一用户日常使用app的统计 每天各种app 的使用时长
    val SOMEONE_DAYLY_USAGE = data.map(x => (x.user_id, x.date, x.apps.map(y => (y.package_name, y.endTime - y.beginTime)).groupBy(_._1).mapValues(_.map(_._2).sum)))

    SOMEONE_DAYLY_USAGE.foreachRDD { rdd =>
      implicit val conn: Connection = HbaseBean.getNewConnection
      rdd.foreachPartition { part =>
        part.foreach { user =>
          HbaseBean.insertRecord("USER", user._1, "usage_statistics", user._2, user._3.map(x => x._1 + ":" + x._2).mkString(","))
        }
      }
    }

    // 某一用户日常使用app的次数的统计
    val SOMEONE_DAYLY_USAGE_TIMES = data.map(x => (x.user_id, x.date, x.apps.length))

    // 存储某一用户日常使用app的次数
    SOMEONE_DAYLY_USAGE_TIMES.foreachRDD { rdd =>
      rdd.foreachPartition { part =>
        implicit val conn: Connection = HbaseBean.getNewConnection
        part.foreach { app =>
          HbaseBean.insertRecord("USER", app._1, "usage_times", app._2, app._3.toString)
        }
      }
    }

    // 某一用户每日的应用历史记录
    // [uid: xx, date: 2018-3-10, [package_name: xx, beginTime: xx]]
    val SOMEONE_DAYLY_HISTORY = data.map(x => (x.user_id, x.date, x.apps.map(y => (y.package_name, y.beginTime))))

    SOMEONE_DAYLY_HISTORY.foreachRDD { rdd =>
      rdd.foreachPartition { part =>
        implicit val conn: Connection = HbaseBean.getNewConnection
        part.foreach { user =>
          HbaseBean.insertRecord("USER", user._1, "usage_history", user._2, user._3.map(x => x._1 + ":" + x._2).mkString(","))
        }
      }
    }

    // 某一用户的兴趣爱好
    // [uid: xx, [hobby: xx, count: 5]]
    val SOMEONE_HIS_HOBBY = data.map(x => (x.user_id, x.apps.map(y => (y.kind, 1)).groupBy(_._1).mapValues(_.map(_._2).sum)))

    SOMEONE_HIS_HOBBY.foreachRDD { rdd =>
      rdd.foreachPartition { part =>
        implicit val conn: Connection = HbaseBean.getNewConnection
        part.foreach { user =>
          val oldSource = HbaseBean.getOneRecord("USER", user._1)
          val oldHobbyMap = scala.collection.mutable.HashMap[String, Int]()
          Option(oldSource.getFamilyMap("hobby".getBytes)).foreach(_ forEach ((k, v) => oldHobbyMap += ((new String(k), new String(v).toInt))))
          val oldHobbyData = oldHobbyMap.toMap
          HbaseBean.insertBatchRecord("USER", user._1, "hobby", oldHobbyData./:(user._2) {
            case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
          } mapValues (_.toString))
        }
      }
    }


    /** ********************************************************************************/
    /** **********************分析用户群体的的应用量(面向所有app) *************************/
    /** ********************************************************************************/

    //---------------------------统计用户群体的兴趣分布-------------------------------
    // 个人用户兴趣分布 按照日期
    // [date: 2018-3-10, [hobby: xx, count: 5]]
    val SINGLE_USER_HOBBY = data.map(x => (x.date, x.apps.map(y => (y.kind, 1)).groupBy(_._1).mapValues(_.map(_._2).sum)))

    // 群体用户兴趣分布 按照日期
    // [date: 2018-3-10, [hobby: xx, count: 5]]
    val USER_GROUP_HOBBY = SINGLE_USER_HOBBY.reduceByKey(_./:(_) {
      case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
    })

    // 存储用户兴趣分布到Hbase
    USER_GROUP_HOBBY.foreachRDD { rdd =>
      rdd.foreachPartition { part =>
        implicit val conn: Connection = HbaseBean.getNewConnection
        part.foreach { piece =>
          val oldSource = HbaseBean.getOneRecord("USER_GROUP", piece._1)
          val oldHobbyMap = scala.collection.mutable.HashMap[String, Int]()
          Option(oldSource.getFamilyMap("hobby".getBytes)).foreach(_ forEach ((k, v) => oldHobbyMap += ((new String(k), new String(v).toInt))))
          val oldHobbyData = oldHobbyMap.toMap
          HbaseBean.insertBatchRecord("USER_GROUP", piece._1, "hobby", oldHobbyData./:(piece._2) {
            case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
          } mapValues (_.toString))
        }
      }
    }

    //---------------------------统计用户群体的地理位置分布----------------------------
    // 用户按照省份分布
    // [date: 2018-3-10, [province: "湖南", count: 12人]]
    val USER_GROUP_LOCATION = data.map(x => (x.date, x.province, 1)).transform(_.groupBy(_._1)).mapValues(_.groupBy(_._2).mapValues(_.map(_._3).sum))

    USER_GROUP_LOCATION.foreachRDD { rdd =>
      rdd.foreachPartition { part =>
        implicit val conn: Connection = HbaseBean.getNewConnection
        part.foreach { location =>
          val oldSource = HbaseBean.getOneRecord("USER_GROUP", location._1)
          val oldHobbyMap = scala.collection.mutable.HashMap[String, Int]()
          Option(oldSource.getFamilyMap("location".getBytes)).foreach(_ forEach ((k, v) => oldHobbyMap += ((new String(k), new String(v).toInt))))
          val oldHobbyData = oldHobbyMap.toMap
          HbaseBean.insertBatchRecord("USER_GROUP", location._1, "location", oldHobbyData./:(location._2) {
            case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
          } mapValues (_.toString))
        }
      }
    }

    //------------------------统计用户群体的每日使用手机时长分布------------------------

    // 单用户每日使用手机时长
    // [date: 2018-3-10, duration: 50000]
    val ONE_USER_DAYLY_DURATION = data.map(x => x.date -> x.apps.map(y => y.endTime - y.beginTime).sum)

    // 单用户每日使用手机时长类别
    // [date: 2018-3-10, kind: 1]
    val ONE_USER_DAYLY_DURATION_MAP = ONE_USER_DAYLY_DURATION.map(x => x._1 -> x._2).map {
      case (k, v) if v >= 0 && v < 300 => (k, "1")
      case (k, v) if v >= 300 && v < 900 => (k, "2")
      case (k, v) if v >= 900 && v < 1800 => (k, "3")
      case (k, v) if v >= 1800 && v < 3600 => (k, "4")
      case (k, v) if v >= 3600 && v < 7200 => (k, "5")
      case (k, _) => (k, "6")
    }

    // 多用户多日期手机使用时长类别
    // [date: 2018-3-10, [kind: 1, count: 5]]
    val USER_GROUP_DAYLY_USAGE = ONE_USER_DAYLY_DURATION_MAP.groupByKey.map {
      case (k, v) => k -> v.map(_ -> 1).groupBy(_._1).mapValues(_.map(_._2).sum)
    }

    // 存入用户群体使用时长类别
    USER_GROUP_DAYLY_USAGE.foreachRDD { rdd =>
      rdd.foreachPartition { part =>
        implicit val conn: Connection = HbaseBean.getNewConnection
        part.foreach { day =>
          val oldSource = HbaseBean.getOneRecord("USER_GROUP", day._1)
          val oldUsageMap = scala.collection.mutable.HashMap[String, Int]()
          Option(oldSource.getFamilyMap("usage".getBytes)).foreach(_ forEach ((k, v) => oldUsageMap += ((new String(k), new String(v).toInt))))
          val oldUsageData = oldUsageMap.toMap
          HbaseBean.insertBatchRecord("USER_GROUP", day._1, "usage", oldUsageData./:(day._2) {
            case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
          }.mapValues(_.toString))
        }
      }
    }

    //------------------------统计用户群体的每日使用手机使用时间分布-----------------------

    // 单人
    // [date: xx, [period: 11, count: 5]]
    val SINGLE_USER_DAYLY_PERIOD = data.map(x => (x.date, x.apps.map(_.beginTime).map { time =>
      val c = Calendar.getInstance()
      c.setTimeInMillis(time)
      (c.get(Calendar.HOUR_OF_DAY).toString, 1)
    }.groupBy(_._1).mapValues(_.map(_._2).sum)))

    // 群体
    // [date: xx, [period: 11, count: 5]]
    val USER_GROUP_DAYLY_PERIOD = SINGLE_USER_DAYLY_PERIOD.reduceByKey(_./:(_) {
      case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
    })

    USER_GROUP_DAYLY_PERIOD.foreachRDD { rdd =>
      rdd.foreachPartition { part =>
        implicit val conn: Connection = HbaseBean.getNewConnection
        part.foreach { piece =>
          val oldSource = HbaseBean.getOneRecord("USER_GROUP", piece._1)
          val oldUsageMap = scala.collection.mutable.HashMap[String, Int]()
          Option(oldSource.getFamilyMap("period".getBytes)).foreach(_ forEach ((k, v) => oldUsageMap += ((new String(k), new String(v).toInt))))
          val oldUsageData = oldUsageMap.toMap
          HbaseBean.insertBatchRecord("USER_GROUP", piece._1, "period", oldUsageData./:(piece._2) {
            case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
          }.mapValues(_.toString))
        }
      }
    }

    ssc.start
    ssc.awaitTermination
  }
}
