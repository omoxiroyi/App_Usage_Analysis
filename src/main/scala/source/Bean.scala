package source

case class DataBean(user_id: String, date: String, apps: Seq[AppUsage], brand: String, model: String, language: String, system_version: String, resolution: String, net_status: String, ISP: String)

case class AppUsage(package_name: String, beginTime: Long, endTime: Long, version: String)

case class Apps(app_name: String, package_name: String)

