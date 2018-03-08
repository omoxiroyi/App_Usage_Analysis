package test

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.random

object gg {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkPi")

    val sc = new SparkContext(conf)

    val n = 1000000L

    val t = sc.parallelize(1L to n).map { x =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)

    println(4 * t / n)

    sc.stop()
  }
}
