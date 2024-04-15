import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import sentiment.mllib.MLlibSentimentAnalyzer.computeSentiment
import utils.{Constants, StopwordsLoader}

object SentimentPredictTest extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("AirlineAnalysis")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .master("local[*]") // Use local mode for development
    .getOrCreate()

  // Set log level to ERROR to reduce console output clutter
  spark.sparkContext.setLogLevel("ERROR")

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
  val totalRows = modifiedDf.count()
  val lastRows = 10
  val skip = totalRows - lastRows

  println(s"Total counts: $totalRows")
  val tailDf = modifiedDf.tail(lastRows)
  tailDf.foreach(println)
  //  println(modifiedDf.printSchema())
  // 创建一个新列，如果两列值相等，则该列值为1，否则为0
  val dfWithInt = modifiedDf.withColumn("sentiment", col("sentiment").cast("int"))
  val comparisonDf = dfWithInt.withColumn("is_equal", when(col("sentiment") === col("sentimentComputed"), 1).otherwise(0))

  // 计算准确率
  val accuracy = comparisonDf.agg(sum("is_equal").cast("double") / count("is_equal")).first().get(0).asInstanceOf[Double]

  println(s"Accuracy: $accuracy")

  // 将tweet_created转换为日期时间类型
  val dfWithTimestamp = comparisonDf.withColumn(
    "tweet_timestamp",
    unix_timestamp(col("tweet_created"), "M/d/yy H:mm").cast(TimestampType)
  )

  // 提取小时
  val dfWithHour = dfWithTimestamp.withColumn("hour", hour(col("tweet_timestamp")))
  val dfWithDateHour = dfWithTimestamp.withColumn(
    "date",
    date_format(col("tweet_timestamp"), "yyyy-MM-dd")
  ).withColumn(
    "hour",
    hour(col("tweet_timestamp"))
  )

  // 按小时分组并计算sentiment和sentimentComputed的平均值
//  val averageSentimentByHour = dfWithHour.groupBy("hour")
//    .agg(
//      round(avg("sentiment"), 2).alias("average_sentiment"),
//      round(avg("sentimentComputed"), 2).alias("average_sentiment_computed")
//    )
//    .orderBy("hour")

//  val averageSentimentByHour = dfWithHour.groupBy("hour")
//    .agg(
//      avg("sentiment").alias("average_sentiment"),
//      avg("sentimentComputed").alias("average_sentiment_computed")
//    )val averageSentimentByDateHour = dfWithDateHour.groupBy("date", "hour")
  //    .agg(
  //      count("*").alias("count"),
  //      round(avg("sentiment"),2).alias("average_sentiment"),
  //      round(avg("sentimentComputed"),2).alias("average_sentiment_computed")
  //    )
  //    .orderBy("date", "hour")
  //
  //  // 显示结果
  //  averageSentimentByDateHour.show(30)
  //
  //  val resultDf = averageSentimentByDateHour
  //    .withColumn("abs_difference", round(abs(col("average_sentiment") - col("average_sentiment_computed")), 2))
  //    .withColumn("squared_difference", round(pow(col("average_sentiment") - col("average_sentiment_computed"), 2), 2))
//    .orderBy("hour")

  val averageSentimentByDateHour = dfWithDateHour.groupBy("date", "hour")
    .agg(
      count("*").alias("count"),
      round(avg("sentiment"),2).alias("average_sentiment"),
      round(avg("sentimentComputed"),2).alias("average_sentiment_computed")
    )
    .orderBy("date", "hour")

  // 显示结果
  averageSentimentByDateHour.show(30)

  val resultDf = averageSentimentByDateHour
    .withColumn("abs_difference", round(abs(col("average_sentiment") - col("average_sentiment_computed")), 2))
    .withColumn("squared_difference", round(pow(col("average_sentiment") - col("average_sentiment_computed"), 2), 2))

//  val resultDf = averageSentimentByDateHour
//    .withColumn("abs_difference", abs(col("average_sentiment") - col("average_sentiment_computed")))
//    .withColumn("squared_difference", pow(col("average_sentiment") - col("average_sentiment_computed"), 2))


  // 显示结果，包括新增的差异度量列
  resultDf.show(30)

  // 指定保存CSV文件的路径
  val outputPath = "src/main/resources/output/average_sentiment_by_hour.csv"

  // 将DataFrame保存为CSV
  resultDf
    .coalesce(1) // 这将所有数据合并到一个分区，从而生成一个CSV文件，但可能不适合大数据集
    .write
    .option("header", "true") // 包含列名作为CSV头部
    .mode("overwrite")
    .csv(outputPath)
}
