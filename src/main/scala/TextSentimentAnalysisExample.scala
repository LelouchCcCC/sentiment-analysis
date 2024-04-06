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

    // 用于广播停用词列表
    val stopWordsList = sc.broadcast(StopwordsLoader.loadStopWords(Constants.STOP_WORDS))

    // 加载模型
    val modelPath = Constants.naiveBayesModelPath
    val naiveBayesModel = NaiveBayesModel.load(sc, modelPath)

    // 加载CSV文件
    val filePath = s"../resources/data/${Constants.TESTING_CSV_FILE_NAME}"
    val df = loadData(spark,filePath)

    // 定义一个UDF，用于调用MLlibSentimentAnalyzer进行情感分析
    val sentimentAnalysisUdf = udf { text: String =>
      MLlibSentimentAnalyzer.computeSentiment(text, stopWordsList, naiveBayesModel)
    }

    // 应用UDF对DataFrame的'text'列进行情感分析
    val sentimentColumnName = "text" // 确保这是包含文本数据的列名
    val analyzedDF = df.withColumn("sentiment", sentimentAnalysisUdf(col(sentimentColumnName)))

    // 显示结果
    analyzedDF.show()

    // 保存结果，更改为实际路径
    analyzedDF.write
      .option("header", "true")
      .csv("path/to/result.csv")
  }
}
