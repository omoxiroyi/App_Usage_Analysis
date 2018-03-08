package streaming

import hbase.HbaseBean
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import play.api.libs.json.Json
import streaming.Util._

object LocalStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[8]").setAppName("Test")
    val ssc = new StreamingContext(conf, Seconds(5))

    val AIV_TABLE: String = "AIV"
    val SINGLE_APP: String = "SINGLE_APP"
    val APP_VERSION: String = "APP_VERSION"

    ssc.checkpoint("hdfs://master:8020/checkpoint")

    val input = ssc.socketTextStream("localhost", 9999)

    //源数据DataBean还原 从Json转化为scala对象
    val data = input.flatMap(Json.parse(_).asOpt[DataBean])

    /** ********************计算应用无关变量 *************************/

    //累计五分钟存储一次应用无关变量数据 app irrelevant variables
    val AIV = data.window(Seconds(20), Seconds(20))

    //将数据按不同日期分类
    /*val DateSplitAIV = AIV.transform(rdd => rdd.groupBy(_.date))

    DateSplitAIV.foreachRDD { rdd =>
      val data = rdd.toLocalIterator

      data.foreach { aiv =>
        val SourceData = HbaseBean.getOneRecord(AIV_TABLE, aiv._1)

        //----------------------统计品牌量----------------------
        val oldBrandMap = scala.collection.mutable.HashMap[String, Int]()
        Option(SourceData.getFamilyMap("brand".getBytes)).foreach(_ forEach ((k, v) => oldBrandMap += ((new String(k), new String(v).toInt))))
        val oldBrandData = oldBrandMap.toMap
        val newBrandData = aiv._2.map(x => (x.brand, 1)).groupBy(_._1).map { case (k, v) => (k, v.map(_._2).sum) }
        // 合并新老品牌数据 存入Hbase
        HbaseBean.insertBatchRecord(AIV_TABLE, aiv._1, "brand", oldBrandData./:(newBrandData) {
          case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
        } mapValues (_.toString))

        //----------------------统计机型量----------------------
        val oldModelMap = scala.collection.mutable.HashMap[String, Int]()
        Option(SourceData.getFamilyMap("model".getBytes)).foreach(_ forEach ((k, v) => oldModelMap += ((new String(k), new String(v).toInt))))
        val oldModelData = oldModelMap.toMap
        val newModelData = aiv._2.map(x => (x.model, 1)).groupBy(_._1).map { case (k, v) => (k, v.map(_._2).sum) }
        // 合并新老机型数据 存入Hbase
        HbaseBean.insertBatchRecord(AIV_TABLE, aiv._1, "model", oldModelData./:(newModelData) {
          case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
        } mapValues (_.toString))

        //----------------------统计系统版本----------------------
        val oldVersionMap = scala.collection.mutable.HashMap[String, Int]()
        Option(SourceData.getFamilyMap("system_version".getBytes)).foreach(_ forEach ((k, v) => oldVersionMap += ((new String(k), new String(v).toInt))))
        val oldVersionData = oldVersionMap.toMap
        val newVersionData = aiv._2.map(x => (x.system_version, 1)).groupBy(_._1).map { case (k, v) => (k, v.map(_._2).sum) }
        HbaseBean.insertBatchRecord(AIV_TABLE, aiv._1, "system_version", oldVersionData./:(newVersionData) {
          case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
        } mapValues (_.toString))

        //----------------------统计分辨率量----------------------
        //----------------------统计联网状态----------------------
        //----------------------统计系统语言----------------------
        //---------------------统计运营商类型---------------------
      }

    }*/

    /** ********************计算单一应用量 *************************/

    // --------------------------统计app每日点击量--------------------------------

    // 格式化数据为每个用户某一天的打开的某个应用的次数 (k, (v._1, v._2)) => k -> 应用的包名 v._1 -> 日期 v._2 -> 应用打开的次数
    // 对应Json数据格式 [(package_name: xx, date: 2018-3-3, count: 2)]
    val SINGLE_USER_DAYLY_DATA = AIV.flatMap(x => x.apps.map(y => (y.package_name, 1)).groupBy(_._1).map { case (k, v) => (k, x.date -> v.map(_._2).sum) })

    // 合并所有的用户数据 格式化为每个应用多天的打开次数情况 (k, map(x, y)) => k -> 应用的包名 x -> 日期 y -> 应用打开的次数
    // 对应Json数据格式 [(package_name: xx, [date: 2018-3-3, count: 5])]
    val ALL_USER_DAYLY_DATA = SINGLE_USER_DAYLY_DATA.groupByKey.map(x => x._1 -> x._2.groupBy(_._1).map { case (k, v) => (k, v.map(_._2).sum) })

    // 存储数据入Hbase
    ALL_USER_DAYLY_DATA.foreachRDD { rdd =>
      rdd.foreach { app =>
        // 查询该App的原数据 rowKey为该app的包名
        val oldSource = HbaseBean.getOneRecord(SINGLE_APP, app._1)
        // 创建map存储原数据
        val oldClickMap = scala.collection.mutable.HashMap[String, Int]()
        Option(oldSource.getFamilyMap("click_num".getBytes)).foreach(_ forEach ((k, v) => oldClickMap += ((new String(k), new String(v).toInt))))
        val oldClickData = oldClickMap.toMap
        // 批量插入合并后的数据
        HbaseBean.insertBatchRecord(SINGLE_APP, app._1, "click_num", oldClickData./:(app._2) {
          case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
        } mapValues (_.toString))
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
      case (k, v) => (k, v.map(_._2).reduce(_./:(_) {
        case (m, (kk, vv)) => m + (kk -> (vv ++ m.getOrElse(kk, Seq[(String, Int)]())))
      }).map(z => (z._1, z._2.groupBy(_._1) map {
        case (kkk, vvv) => (kkk, vvv.map(_._2).sum)
      })))
    }

    // 存储app版本应用量
    ALL_APP_VERSION.foreachRDD { rdd =>
      rdd.foreach { app =>
        app._2.foreach { version =>
          // 无论如何 先存入版本号入SINGLE_APP表中的app_version列族 方便查询版本时索引
          HbaseBean.insertRecord(SINGLE_APP, app._1, "app_version", version._1, version._1)
          // 查询该App的原数据 rowKey为该app的包名加版本号
          val rowKey = app._1 + "_" + version._1
          val oldSource = HbaseBean.getOneRecord(APP_VERSION, rowKey)
          val oldClickMap = scala.collection.mutable.HashMap[String, Int]()
          Option(oldSource.getFamilyMap("click_num".getBytes)).foreach(_ forEach ((k, v) => oldClickMap += ((new String(k), new String(v).toInt))))
          val oldClickData = oldClickMap.toMap
          HbaseBean.insertBatchRecord(APP_VERSION, rowKey, "click_num", oldClickData./:(version._2) {
            case (m, (k, v)) => m + (k -> (v + m.getOrElse(k, 0)))
          } mapValues (_.toString))
        }
      }
    }



    ssc.start
    ssc.awaitTermination
  }
}
