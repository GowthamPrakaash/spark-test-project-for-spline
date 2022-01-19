package com.hackathon.demo.ETL

import org.apache.spark.sql.{SaveMode, SparkSession}

class Sample(spark:SparkSession, inputFile:String, outputPath:String) {
  //Aggregate based on sgrp
  def transform: Unit ={

    val df = spark.read.option("header", true).csv(inputFile)
    //df.show(20)
    df.createOrReplaceTempView("DataFrame")
    val OutputDf = spark.sql(
      """
        |select a.month, a.storecode, a.price as unit_price, sum(a.qty) as total_qty,
        |sum(a.value) as total_price, a.sgrp, a.ssgrp, a.cmp, a.mbrd, a.brd
        |from DataFrame a
        |group by month, storecode, brd, mbrd,cmp, ssgrp, sgrp, price, qty, value
      """.stripMargin)
    println(OutputDf.show(20))
    val fullParquetPath = outputPath + "Process1"
    OutputDf.coalesce(10).write.mode(SaveMode.Overwrite).parquet(fullParquetPath)
    println("Process1 completed!!!")
  }

}
