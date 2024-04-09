import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
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
  modifiedDf.show(400)
//  println(modifiedDf.printSchema())
  // 创建一个新列，如果两列值相等，则该列值为1，否则为0
  val dfWithInt = modifiedDf.withColumn("sentiment", col("sentiment").cast("int"))
  val comparisonDf = dfWithInt.withColumn("is_equal", when(col("sentiment") === col("sentimentComputed"), 1).otherwise(0))

  // 计算准确率
  val accuracy = comparisonDf.agg(sum("is_equal").cast("double") / count("is_equal")).first().get(0).asInstanceOf[Double]

  println(s"Accuracy: $accuracy")
}
