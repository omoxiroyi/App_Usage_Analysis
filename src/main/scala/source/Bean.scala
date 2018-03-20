package source

case class DataBean(user_id: String, province: String, city: String, date: String, apps: Seq[AppUsage], brand: String, model: String, language: String, system_version: String, resolution: String, net_status: String, ISP: String)

case class AppUsage(package_name: String, beginTime: Long, endTime: Long, version: String, kind: String)