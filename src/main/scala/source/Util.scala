package source

import java.sql.{Connection, DriverManager}

import anorm.SqlParser._
import anorm._
import play.api.libs.json.{Json, OFormat}


object Util {
  private val url = "jdbc:mysql://localhost:3306/phonedata?useSSL=false&characterEncoding=utf-8"
  private val username = "root"
  private val password = "199729"

  case class App(app_name: String, package_name: String, count: Int, kind: String)

  case class BrandModel(brand: String, model: String, count: Int)

  case class SystemVersion(version: String, count: Int)

  case class Language(language: String, count: Int)

  case class ISP(name: String, count: Int)

  case class Resolution(resolution: String, count: Int)

  case class NetStatus(name: String, count: Int)

  case class User(uid: String, province: String, city: String)

  lazy val AppSample: RowParser[App] = get[String]("app_name") ~
    get[String]("package_name") ~
    get[Int]("count") ~
    get[String]("kind") map {
    case app_name ~ package_name ~ count ~ kind => App(app_name, package_name, count, kind)
  }

  lazy val BrandModelSample: RowParser[BrandModel] = get[String]("brand") ~
    get[String]("model") ~
    get[Int]("count") map {
    case brand ~ model ~ count => BrandModel(brand, model, count)
  }

  lazy val systemVersionSample: RowParser[SystemVersion] = get[String]("version") ~ get[Int]("count") map {
    case system_version ~ count => SystemVersion(system_version, count)
  }

  lazy val languageSample: RowParser[Language] = get[String]("language") ~ get[Int]("count") map {
    case language ~ count => Language(language, count)
  }

  lazy val ISPSample: RowParser[ISP] = get[String]("name") ~ get[Int]("count") map {
    case name ~ count => ISP(name, count)
  }

  lazy val ResolutionSample: RowParser[Resolution] = get[String]("resolution") ~ get[Int]("count") map {
    case resolution ~ count => Resolution(resolution, count)
  }

  lazy val NetStatusSample: RowParser[NetStatus] = get[String]("name") ~ get[Int]("count") map {
    case net_status ~ count => NetStatus(net_status, count)
  }

  lazy val UserSample: RowParser[User] = get[String]("id") ~ get[String]("province") ~ get[String]("city") map {
    case id ~ province ~ city => User(id, province, city)
  }

  implicit lazy val conn: Connection = DriverManager.getConnection(url, username, password)

  implicit lazy val AppsFormat: OFormat[AppUsage] = Json.format[AppUsage]

  implicit lazy val DataBeanFormat: OFormat[DataBean] = Json.format[DataBean]
}
