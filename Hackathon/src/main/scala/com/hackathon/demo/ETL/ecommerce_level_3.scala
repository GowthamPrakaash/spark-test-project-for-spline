package com.hackathon.demo.ETL

import org.apache.spark.sql.{SaveMode, SparkSession}

class ecommerce_level_3(spark:SparkSession, path:String) {
  def transform: Unit = {

    val df = spark.read.parquet(path+"/seller_products")
    val order_reviews_df = spark.read.parquet(path+"/order_reviews")

    df.createOrReplaceTempView("seller_products")
    order_reviews_df.createOrReplaceTempView("order_reviews")

    val OutputDf = spark.sql(
      """
        |with cte as
        |(
        |    select r.order_id, avg(review_score) as avg_review_score
        |    from seller_products s
        |    inner join order_reviews r on (s.order_id = r.order_id)
        |    group by r.order_id
        |)
        |select s.*, c.avg_review_score
        |from seller_products s
        |inner join cte c on (s.order_id = c.order_id)
      """
        .stripMargin)
    println(OutputDf.show(20))
        val fullParquetPath = path + "/review_scores"
    OutputDf.coalesce(10).write.mode(SaveMode.Overwrite).parquet(fullParquetPath)
    println("Level 3 ETL completed!!!")
  }
}
