package dataProcessor

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.functions._

import java.text.SimpleDateFormat


object DataProcessor {
  def loadData(spark: SparkSession, path: String): DataFrame = {
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

    val isValidDate = udf((date: String) => {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z")
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
    val adjustedDf = filteredDf.withColumn("tweet_created", to_timestamp($"tweet_created", "yyyy-MM-dd HH:mm:ss Z"))
    changeTime(adjustedDf,spark)
  }



  // change some time

  def changeTime(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._

    // find max date
    val maxDate = df.select(max($"tweet_created")).as[String].collect()(0)

    if (maxDate != null) {
      val maxDateTime = unix_timestamp(lit(maxDate), "yyyy-MM-dd HH:mm:ss").cast("timestamp")

      // calculate yestoday
      val yesterday = current_date().cast("timestamp")

      val diff = datediff(yesterday, maxDateTime)

      // process the time
      df.withColumn("date", to_date($"tweet_created", "yyyy-MM-dd"))
        .withColumn("time", date_format($"tweet_created", "HH:mm:ss"))
        .withColumn("new_date", date_add($"date", diff))
        .withColumn("new_tweet_created", concat_ws(" ", $"new_date", $"time"))
        .drop("date", "time", "new_date", "tweet_created")
        .withColumnRenamed("new_tweet_created", "tweet_created")
    } else {
      df
    }
  }


}
