package com.hackathon.demo.ETL

import org.apache.spark.sql.{SaveMode, SparkSession}

class ecommerce_level_1(spark:SparkSession, inputPath:String, outputPath:String) {
  def transform: Unit ={

    // read input files
    val order_items_df = spark.read.option("header", true).csv(inputPath+"/olist_order_items_dataset.csv")
    val order_reviews_df = spark.read.option("header", true).csv(inputPath+"/olist_order_reviews_dataset.csv")
    val orders_df = spark.read.option("header", true).csv(inputPath+"/olist_orders_dataset.csv")
    val products_df = spark.read.option("header", true).csv(inputPath+"/olist_products_dataset.csv")
    val sellers_df = spark.read.option("header", true).csv(inputPath+"/olist_sellers_dataset.csv")

    // level 1 copy of three data sets
    sellers_df.coalesce(10).write.mode(SaveMode.Overwrite).parquet(outputPath+"/sellers")
    order_reviews_df.coalesce(10).write.mode(SaveMode.Overwrite).parquet(outputPath+"/order_reviews")

    // transform
    products_df.createOrReplaceTempView("products")
    order_items_df.createOrReplaceTempView("order_items")
    orders_df.createOrReplaceTempView("orders")

    // join and transform three data sets
    val OutputDf = spark.sql(
      """
        |with cte as (
        |    select
        |    o.order_id, p.product_id, oi.order_item_id, oi.seller_id, oi.price
        |    ,row_number() over (partition by p.product_id, o.order_id order by order_purchase_timestamp) as rn
        |    from products p
        |    inner join order_items oi on p.product_id = oi.product_id
        |    inner join orders o on o.order_id = oi.order_id
        |)
        |select
        |*
        |from cte
        |where rn = 1
      """
      .stripMargin)
    println(OutputDf.show(20))
    val fullParquetPath = outputPath + "/ordered_products"
    OutputDf.coalesce(10).write.mode(SaveMode.Overwrite).parquet(fullParquetPath)
    println("Level 1 ETL completed!!!")
  }

}
