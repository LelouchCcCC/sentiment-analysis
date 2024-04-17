import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import sentiment.mllib.MLlibSentimentAnalyzer
import dataProcessor.DataProcessor.{loadData,changeTime}
import utils.{Constants, StopwordsLoader}
import org.apache.spark.mllib.classification.NaiveBayesModel

object TextSentimentAnalysisExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Text Sentiment Analysis")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // load stop words
    val stopWordsList = sc.broadcast(StopwordsLoader.loadStopWords(Constants.STOP_WORDS))

    // load model
    val modelPath = Constants.naiveBayesModelPath
    val naiveBayesModel = NaiveBayesModel.load(sc, modelPath)

    // load csv
    val filePath = s"../resources/data/${Constants.TESTING_CSV_FILE_NAME}"
    val df = loadData(spark,filePath)

    // use udf to compute sentiment value
    val sentimentAnalysisUdf = udf { text: String =>
      MLlibSentimentAnalyzer.computeSentiment(text, stopWordsList, naiveBayesModel)
    }

    val sentimentColumnName = "text"
    val analyzedDF = df.withColumn("sentiment", sentimentAnalysisUdf(col(sentimentColumnName)))

    // show result
    analyzedDF.show()

    // 保存结果，更改为实际路径
    analyzedDF.write
      .option("header", "true")
      .csv("path/to/result.csv")
  }
}
