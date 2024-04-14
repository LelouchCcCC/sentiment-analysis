package utils



import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.sql.SparkSession
import sentiment.mllib.MLlibSentimentAnalyzer

object SentimentAnalyzer {
  def singleAnalyze(spark: SparkSession, text: String): Int = {
    if (text == null) {
      throw new IllegalArgumentException("Input text cannot be null")
    }

    val sc = spark.sparkContext
    val stopWordsList = sc.broadcast(StopwordsLoader.loadStopWords("NLTK_English_Stopwords_Corpus.txt"))
    if (stopWordsList.value == null) {
      throw new IllegalStateException("Failed to load stop words")
    }

    val modelPath = "src/main/scala/model"
    val naiveBayesModel = NaiveBayesModel.load(sc, modelPath)

    if (naiveBayesModel == null) {
      throw new IllegalStateException(s"Failed to load Naive Bayes Model from path: $modelPath")
    }

    MLlibSentimentAnalyzer.computeSentiment(text, stopWordsList, naiveBayesModel)
  }
}
