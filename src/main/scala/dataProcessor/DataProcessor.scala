package dataProcessor
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.sql.functions.{current_timestamp, date_add, datediff, max}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions => F}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import sentiment.mllib.MLlibSentimentAnalyzer.computeSentiment
import utils.{Constants, StopwordsLoader}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}


object DataProcessor {
  def loadData(spark: SparkSession, path: String): DataFrame = {
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("multiline", "true")
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

//    val filteredDf = df.filter(isValidDate(df("tweet_created")))
//    import spark.implicits._
//    val adjustedDf = filteredDf.withColumn("tweet_created", to_timestamp($"tweet_created", "MM/dd/yy HH:mm"))
//    val finalDf = adjustedDf.na.drop()
//    val res = changeTime(finalDf, spark)
//      .withColumn("text", regexp_replace(col("text"), "\r?\n", " "))
//      .select("sentiment", "text")
//      .filter(col("text").isNotNull && col("sentiment").isNotNull)
//    res
    df.na.drop()

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


  def retn_data(spark:SparkSession) = {
    val sc = spark.sparkContext

    // 用于广播停用词列表
    val stopWordsList = sc.broadcast(StopwordsLoader.loadStopWords(Constants.STOP_WORDS))

    val airlineData = spark.read.option("inferSchema", "true").option("header", "true").csv("src/main/resources/data/airline.csv")

    // 加载模型
    val modelPath = Constants.naiveBayesModelPath
    val naiveBayesModel = NaiveBayesModel.load(sc, modelPath)
    //  println(airlineData.show(300))
    val filteredAirlineData = airlineData.filter(col("airline_sentiment").rlike("^(neutral|positive|negative)$"))
    val dropNAAirline = filteredAirlineData.na.drop()
    //  println(dropNAAirline.show(30))
    val processText = udf((s: String) => computeSentiment(s, stopWordsList, naiveBayesModel))
    val modifiedDf = dropNAAirline.withColumn("sentimentComputed", processText(col("text")))
    modifiedDf.show(400)
    //  println(modifiedDf.printSchema())
    // 创建一个新列，如果两列值相等，则该列值为1，否则为0
    val dfWithInt = modifiedDf.withColumn("sentiment", col("sentiment").cast("int"))
    val comparisonDf = dfWithInt.withColumn("is_equal", when(col("sentiment") === col("sentimentComputed"), 1).otherwise(0))

    // 计算准确率
    val accuracy = comparisonDf.agg(sum("is_equal").cast("double") / count("is_equal")).first().get(0).asInstanceOf[Double]

    println(s"Accuracy: $accuracy")

    // 将tweet_created转换为日期时间类型
    val dfWithTimestamp = comparisonDf.withColumn("tweet_timestamp", unix_timestamp(col("tweet_created"), "M/d/yy H:mm").cast(TimestampType))

    // 提取小时
    val dfWithHour = dfWithTimestamp.withColumn("hour", hour(col("tweet_timestamp")))

    // 按小时分组并计算sentiment和sentimentComputed的平均值
    val averageSentimentByHour = dfWithHour.groupBy("hour")
      .agg(
        round(avg("sentiment"), 2).alias("average_sentiment"),
        round(avg("sentimentComputed"), 2).alias("average_sentiment_computed")
      )
      .orderBy("hour")

    // 显示结果
    averageSentimentByHour.show()

    val resultDf = averageSentimentByHour
      .withColumn("abs_difference", round(abs(col("average_sentiment") - col("average_sentiment_computed")), 2))
      .withColumn("squared_difference", round(pow(col("average_sentiment") - col("average_sentiment_computed"), 2), 2))


    // 显示结果，包括新增的差异度量列
    resultDf.show()

    val jsonStrings = resultDf.toJSON.collect().mkString("[", ",", "]")
    jsonStrings
  }


}
