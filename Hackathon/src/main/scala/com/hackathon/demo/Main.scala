package com.hackathon.demo

import com.hackathon.demo.ETL._
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf
import za.co.absa.spline.harvester.SparkLineageInitializer._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val inputPath = opt[String](required = true, name = "inputPath", argName = "inputPath", descr = "Input csv file location", default = Some("Hackathon/src/main/resources/data"))
  val outputPath = opt[String](required = false, name = "outputPath", argName = "outputPath", descr = "Output path location for storing output parquets", default = Some("Hackathon/src/main/resources/output"))
  val level = opt[Int](required = true, name = "level", argName = "level", descr = "Mention the level of ETL to run", default = Some(1))
  verify()
}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val inputPath = conf.inputPath()
    val outputPath = conf.outputPath()
    val level = conf.level()
    val spark:SparkSession = SparkSession.builder().master("local").appName("ETL"+level).getOrCreate()
    spark.enableLineageTracking()

    level match {
      case 1 => {
        val proc1 = new ecommerce_level_1(spark, inputPath, outputPath)
        proc1.transform
      }
      case 2 => {
        val proc1 = new ecommerce_level_2(spark, inputPath)
        proc1.transform
      }
      case 3 => {
        val proc1 = new ecommerce_level_3(spark, inputPath)
        proc1.transform
      }
      case _ => {
        val proc1 = new Sample(spark, inputPath, outputPath)
        proc1.transform
      }
    }
  }
}
