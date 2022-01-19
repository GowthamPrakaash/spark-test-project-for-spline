package com.hackathon.demo.ETL

import org.apache.spark.sql.{SaveMode, SparkSession}

class ecommerce_level_2(spark:SparkSession, path:String) {
  def transform: Unit = {

    val order_products_df = spark.read.parquet(path+"/ordered_products")
    val sellers_df = spark.read.parquet(path+"/sellers")

    order_products_df.createOrReplaceTempView("ordered_products")
    sellers_df.createOrReplaceTempView("sellers")

    val OutputDf = spark.sql(
      """
        |with cte as
        |(
        |    select s.seller_id, op.product_id, op.order_id,
        |    count(op.order_item_id) over(partition by s.seller_id, op.product_id, op.order_id) as products_sold,
        |    row_number() over (partition by s.seller_id, op.product_id, op.order_id, s.seller_state order by s.seller_state) as rn
        |    from ordered_products op
        |    inner join sellers s on (op.seller_id = s.seller_id)
        |)
        |select
        |   c.*, seller_zip_code_prefix, seller_city
        |from cte c
        |join sellers s on (c.seller_id = s.seller_id)
        |where c.rn  = 1
        |order by products_sold
      """
        .stripMargin)
    println(OutputDf.show(20))
    val fullParquetPath = path + "/seller_products"
    OutputDf.coalesce(10).write.mode(SaveMode.Overwrite).parquet(fullParquetPath)
    println("Level 2 ETL completed!!!")
  }
}
