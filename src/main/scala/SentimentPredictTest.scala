import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}
import sentiment.mllib.MLlibSentimentAnalyzer.computeSentiment
import utils.{Constants, StopwordsLoader}

object SentimentPredictTest extends App {
  val spark: SparkSession = buildSparkSession()

  import spark.implicits._

  try {
    processAirlineData(spark)
  } finally {
    spark.stop()
  }

  def buildSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("AirlineAnalysis")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .master("local[*]")
      .getOrCreate()
  }

  def processAirlineData(spark: SparkSession): Unit = {
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val stopWordsList = sc.broadcast(StopwordsLoader.loadStopWords(Constants.STOP_WORDS))
    val naiveBayesModel = NaiveBayesModel.load(sc, Constants.naiveBayesModelPath)

    val airlineData = loadData(spark)
    val sentimentData = computeSentimentAnalysis(airlineData, stopWordsList, naiveBayesModel)
    calculateAccuracy(sentimentData)
    val analysisResults = performAnalysis(sentimentData)
    analysisResults.show(30)
    saveResults(analysisResults, Constants.NEW_CSV_FILE_FOLDER)
  }

  def loadData(spark: SparkSession): DataFrame = {
    spark.read.option("inferSchema", "true").option("header", "true")
      .csv("src/main/resources/" + Constants.TESTING_CSV_FILE_NAME)
      .filter(col("airline_sentiment").rlike("^(neutral|positive|negative)$"))
      .na.drop()
  }

  def computeSentimentAnalysis(df: DataFrame, stopWordsList: Broadcast[List[String]], model: NaiveBayesModel): DataFrame = {
    val processText = udf((s: String) => computeSentiment(s, stopWordsList, model))
    df.withColumn("sentimentComputed", processText(col("text")))
      .withColumn("sentiment", col("sentiment").cast("int"))
      .withColumn("is_equal", when(col("sentiment") === col("sentimentComputed"), 1).otherwise(0))
  }

  def calculateAccuracy(df: DataFrame): Unit = {
    val accuracyResult = df.agg((sum("is_equal").cast("double") / count("is_equal")).alias("accuracy")).first()
    val accuracy = accuracyResult.getAs[Double]("accuracy")
    println(s"Accuracy: $accuracy")
  }

  def performAnalysis(df: DataFrame): DataFrame = {
    df.withColumn("tweet_timestamp", unix_timestamp(col("tweet_created"), "M/d/yy H:mm").cast(TimestampType))
      .withColumn("hour", hour($"tweet_timestamp"))
      .withColumn("date", date_format($"tweet_timestamp", "yyyy-MM-dd"))
      .groupBy("date", "hour")
      .agg(
        count("*").alias("count"),
        round(avg("sentiment"), 2).alias("average_sentiment"),
        round(avg("sentimentComputed"), 2).alias("average_sentiment_computed"),
        round(abs(col("average_sentiment") - col("average_sentiment_computed")), 2).alias("abs_difference"),
        round(pow(col("average_sentiment") - col("average_sentiment_computed"), 2), 2).alias("squared_difference")
      )
      .orderBy("date", "hour")
  }

  def saveResults(df: DataFrame, path: String): Unit = {
    df.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(path)
  }
}
