package test

import java.util.Calendar

object gg {
  def main(args: Array[String]): Unit = {
    val today = Calendar.getInstance
    today.set(Calendar.YEAR, 2016)
    today.set(Calendar.MONTH, 5)
    today.set(Calendar.DAY_OF_MONTH, 1)
    println(today.getTime)
  }
}
