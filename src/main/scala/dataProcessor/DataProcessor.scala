package dataProcessor
import org.apache.spark.sql.functions.{current_timestamp, date_add, datediff, max}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions => F}
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}


object DataProcessor {
  def loadData(spark: SparkSession, path: String): DataFrame = {
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

    val isValidDate = udf((date: String) => {
      val dateFormat = new SimpleDateFormat("MM/dd/yy HH:mm")
      dateFormat.setLenient(false)
      try {
        dateFormat.parse(date)
        true
      } catch {
        case _: Exception => false
      }
    })

    val filteredDf = df.filter(isValidDate(df("tweet_created")))
    import spark.implicits._
    val adjustedDf = filteredDf.withColumn("tweet_created", to_timestamp($"tweet_created", "MM/dd/yy HH:mm"))
    val finalDf = adjustedDf.na.drop()
    val res = changeTime(finalDf, spark)
    res.show(20)
    res
  }

  def changeTime(df: DataFrame, spark: SparkSession): DataFrame = {
    df.createOrReplaceTempView("tweets")

    // 获取昨天的日期
    val yesterday = LocalDate.now(ZoneId.systemDefault()).minusDays(1)

    // 使用 SQL 语句调整日期，保留时间不变
    val query =
      s"""
      SELECT *,
             to_timestamp(concat(date_format(to_date('$yesterday'), 'yyyy-MM-dd'), ' ', date_format(tweet_created, 'HH:mm:ss'))) as adjusted_tweet_created
      FROM tweets
    """
    val adjustedDf = spark.sql(query)

    adjustedDf.drop("tweet_created").withColumnRenamed("adjusted_tweet_created", "tweet_created")
  }


}
