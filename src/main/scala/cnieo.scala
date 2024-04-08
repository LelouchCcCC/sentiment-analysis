import dataProcessor.DataProcessor.loadData
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.sql.SparkSession
import sentiment.corenlp.SparkNaiveBayesModelCreator.validateAccuracyOfNBModel
import sentiment.mllib.MLlibSentimentAnalyzer.computeSentiment
import utils.{Constants, StopwordsLoader}

object cnieo extends App{
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

  // 加载模型
  val modelPath = Constants.naiveBayesModelPath
  val naiveBayesModel = NaiveBayesModel.load(sc, modelPath)

  val text = "Thank you very much"

  println(computeSentiment(text,stopWordsList,naiveBayesModel));



}
