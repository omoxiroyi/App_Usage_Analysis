package source

import java.io.PrintWriter
import java.net.ServerSocket
import java.text.SimpleDateFormat
import java.util.Calendar

import anorm._
import play.api.libs.json.Json
import source.Util._

import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.util.Random

object Source {
  def main(args: Array[String]): Unit = {
    val apps = SQL("select * from temp").as(AppSample *)
    val brands = SQL("select * from brand_model").as(BrandModelSample *)
    val systemVersions = SQL("select * from system_version").as(systemVersionSample *)
    val languages = SQL("select * from language").as(languageSample *)
    val users = SQL("select * from user_id").as(UserSample *)
    val ISPs = SQL("select * from ISP").as(ISPSample *)
    val resolutions = SQL("select * from resolution").as(ResolutionSample *)
    val net_status = SQL("select * from net_status").as(NetStatusSample *)
    val today = Calendar.getInstance
    val formatter = new SimpleDateFormat("yyyy-MM-dd")

    val serverSocket = new ServerSocket(9999)
    val socket = serverSocket.accept
    println("连接成功")
    val out = new PrintWriter(socket.getOutputStream)

    while (true) {
      val date = formatter.format(today.getTime)
      today.add(Calendar.DAY_OF_MONTH, -1)
      1 to 100 foreach { _ =>
        val brand_model = getRandomBrand(brands)
        val user = getRandomUser(users)
        val data = DataBean(user.uid, user.province, user.city, date, getRandomAppUsage(apps), brand_model._1, brand_model._2, getRandomLanguage(languages), getRandomSystemVersion(systemVersions), getRandomResolution(resolutions), getRandomNetStatus(net_status), getRandomISP(ISPs))
        out.println(Json.toJson(data))
      }
      out.flush()
      Thread.sleep(1000) //*5
    }
  }

  def getRandomISP(ISPs: List[ISP]): String = {
    var cnt = 0
    val weight = ISPs.map { x =>
      cnt = cnt + x.count
      (x.name, cnt)
    }
    val rand = Random.nextInt(cnt) + 1
    weight.find(_._2 >= rand).map(_._1).get
  }

  def getRandomNetStatus(NetStatus: List[NetStatus]): String = {
    var cnt = 0
    val weight = NetStatus.map { x =>
      cnt = cnt + x.count
      (x.name, cnt)
    }
    val rand = Random.nextInt(cnt) + 1
    weight.find(_._2 >= rand).map(_._1).get
  }

  def getRandomResolution(resolutions: List[(Resolution)]): String = {
    var cnt = 0
    val weight = resolutions.map { x =>
      cnt = cnt + x.count
      (x.resolution, cnt)
    }
    val rand = Random.nextInt(cnt) + 1
    weight.find(_._2 >= rand).map(_._1).get
  }

  def getRandomBrand(brands: List[BrandModel]): (String, String) = {
    val single = brands.map(x => (x.brand, x.count)).groupBy(_._1).map {
      case (k, v) => (k, v.map(_._2).sum)
    }
    var cnt = 0
    var weight = single.map { x =>
      cnt = cnt + x._2
      (x._1, cnt)
    }
    var rand = Random.nextInt(cnt) + 1
    val brand = weight.find(_._2 >= rand).map(_._1).get
    val models = brands.filter(x => x.brand.equals(brand)).map(x => (x.model, x.count)).groupBy(_._1).map {
      case (k, v) => (k, v.map(_._2).sum)
    }
    cnt = 0
    weight = models.map { x =>
      cnt = cnt + x._2
      (x._1, cnt)
    }
    rand = Random.nextInt(cnt) + 1
    val model = weight.find(_._2 >= rand).map(_._1).get
    (brand, model)
  }

  def getRandomSystemVersion(systemVersions: List[SystemVersion]): String = {
    var cnt = 0
    val weight = systemVersions.map { x =>
      cnt = cnt + x.count
      (x.version, cnt)
    }
    val rand = Random.nextInt(cnt) + 1
    weight.find(_._2 >= rand).map(_._1).get
  }

  def getRandomLanguage(languages: List[Language]): String = {
    var cnt = 0
    val weight = languages.map { x =>
      cnt = cnt + x.count
      (x.language, cnt)
    }
    val rand = Random.nextInt(cnt) + 1
    weight.find(_._2 >= rand).map(_._1).get
  }

  def getRandomUser(users: List[User]): User = users(Random.nextInt(users.length))

  def getRandomAppUsage(apps: List[App]): List[AppUsage] = {
    val date = Calendar.getInstance
    date.set(Calendar.HOUR_OF_DAY, 0)
    date.set(Calendar.MINUTE, 0)
    date.set(Calendar.SECOND, 0)
    date.set(Calendar.MILLISECOND, 0)
    val beginTime = date.getTimeInMillis
    date.add(Calendar.DAY_OF_MONTH, 1)
    val endTime = date.getTimeInMillis
    var current = beginTime

    var cnt = 0
    val weight = apps.map { x =>
      cnt = cnt + x.count
      (x.app_name, x.package_name, x.kind, cnt)
    }

    val buffer = ListBuffer[AppUsage]()

    val versionList = List[(String, Int)]("1.0" -> 1, "1.1" -> 3, "2.0" -> 5, "3.0" -> 7, "4.0" -> 9)

    var c = 0
    val w = versionList.map { x =>
      c = c + x._2
      (x._1, c)
    }

    while (current < endTime) {
      val ifUse = Random.nextInt(3)
      val randTime = Random.nextInt(3000000) + 5000
      if (ifUse == 0) {
        val rand = Random.nextInt(cnt) + 1
        val app = weight.find(_._4 >= rand).map(x => (x._1, x._2, x._3)).get

        val r = Random.nextInt(c) + 1

        val version = w.find(_._2 >= r).map(_._1).get

        buffer += AppUsage(app._2, current, current + randTime, version, app._3)
      }
      current = current + randTime
    }
    buffer.toList
  }

}
