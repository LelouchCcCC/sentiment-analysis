package sentiment.modelCreator


import dataProcessor.DataProcessor.loadData
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import sentiment.mllib.MLlibSentimentAnalyzer
import sentiment.mllib.MLlibSentimentAnalyzer.normalizeMLlibSentiment
import utils.{Constants, SQLContextSingleton, StopwordsLoader}



/**
 * Creates a Model of the training dataset using Spark MLlib's Naive Bayes classifier.
 */

object SparkNaiveBayesModelCreator {

  val trainFilePath = s"src/main/resources/${Constants.TRAINING_CSV_FILE_NAME}"
  val testFilePath = s"src/main/resources/${Constants.TESTING_CSV_FILE_NAME}"

  def main(args: Array[String]) {
    val sc = createSparkContext()

    //LogUtils.setLogLevels(sc)

    val stopWordsList = sc.broadcast(StopwordsLoader.loadStopWords(Constants.STOP_WORDS))
    createAndSaveNBModel(sc, stopWordsList)
    //validateAccuracyOfNBModel(sc, stopWordsList)
  }

  /**
   * Remove new line characters.
   *
   * @param tweetText -- Complete text of a tweet.
   * @return String with new lines removed.
   */
  def replaceNewLines(tweetText: String): String = {
    tweetText.replaceAll("\n", "")
  }

  /**
   * Create SparkContext.
   * Future extension: enable checkpointing to HDFS [is it really reqd??].
   *
   * @return SparkContext
   */
  def createSparkContext(): SparkContext = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
    val sc = SparkContext.getOrCreate(conf)
    sc
  }

  def createSparkSession(): SparkSession = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("AirlineAnalysis")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .master("local[*]") // Use local mode for development
      .getOrCreate()

    // Set log level to ERROR to reduce console output clutter
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  /**
   * Creates a Naive Bayes Model of Tweet and its Sentiment from the Sentiment140 file.
   *
   * @param sc            -- Spark Context.
   * @param stopWordsList -- Broadcast variable for list of stop words to be removed from the tweets.
   */
  def createAndSaveNBModel(sc: SparkContext, stopWordsList: Broadcast[List[String]]): Unit = {

    //加载train用的data set
    val tweetsDF: DataFrame = loadData(createSparkSession(), trainFilePath).select("sentiment",  "text")
    tweetsDF.printSchema()
    tweetsDF.show(20)


    val labeledRDD = tweetsDF.select("sentiment", "text").rdd

    val processedRDD = labeledRDD.flatMap {
      case Row(sentimentString: Int, tweet: String) =>
        try {
          val sentiment = sentimentString.toInt
          val tweetInWords: Seq[String] = MLlibSentimentAnalyzer.getBarebonesTweetText(tweet, stopWordsList.value)
          if (tweetInWords.nonEmpty) {
            Some(LabeledPoint(sentiment.toDouble, MLlibSentimentAnalyzer.transformFeatures(tweetInWords)))
          } else {
            None
          }
        } catch {
          case e: Exception => None
        }
      case _ => None
    }

    processedRDD.cache()

    // 训练朴素贝叶斯模型
    val naiveBayesModel: NaiveBayesModel = NaiveBayes.train(processedRDD, lambda = 1.0, modelType = "multinomial")
    naiveBayesModel.save(sc, Constants.naiveBayesModelPath)
  }

  /**
   * Validates and check the accuracy of the model by comparing the polarity of a tweet from the dataset and compares it with the MLlib predicted polarity.
   *
   * @param sc            -- Spark Context.
   * @param stopWordsList -- Broadcast variable for list of stop words to be removed from the tweets.
   */
  def validateAccuracyOfNBModel(sc: SparkContext, stopWordsList: Broadcast[List[String]]): Unit = {
    val naiveBayesModel: NaiveBayesModel = NaiveBayesModel.load(sc, Constants.naiveBayesModelPath)

    val tweetsDF: DataFrame = loadData(createSparkSession(), testFilePath)
    val actualVsPredictionRDD = tweetsDF.select("sentiment", "text").rdd.map {
      case Row(polarityStr: String, tweet: String) =>
        val polarity = polarityStr.toDouble
        val tweetText = replaceNewLines(tweet)
        val tweetInWords: Seq[String] = MLlibSentimentAnalyzer.getBarebonesTweetText(tweetText, stopWordsList.value)
        val predicted = naiveBayesModel.predict(MLlibSentimentAnalyzer.transformFeatures(tweetInWords))
//        println(polarity,predicted)
        if (predicted==4.0) println(tweetText)
        (polarity,
          normalizeMLlibSentiment(predicted).toDouble,
          tweetText)
    }
    val accuracy = 100.0 * actualVsPredictionRDD.filter(x => x._1 == x._2).count() / tweetsDF.count()
    /*actualVsPredictionRDD.cache()
    val predictedCorrect = actualVsPredictionRDD.filter(x => x._1 == x._2).count()
    val predictedInCorrect = actualVsPredictionRDD.filter(x => x._1 != x._2).count()
    val accuracy = 100.0 * predictedCorrect.toDouble / (predictedCorrect + predictedInCorrect).toDouble*/
    println(f"""\n\t<==******** Prediction accuracy compared to actual: $accuracy%.2f%% ********==>\n""")
    saveAccuracy(sc, actualVsPredictionRDD)
  }



  /**
   * Saves the accuracy computation of the ML library.
   * The columns are actual polarity as per the dataset, computed polarity with MLlib and the tweet text.
   *
   * @param sc                    -- Spark Context.
   * @param actualVsPredictionRDD -- RDD of polarity of a tweet in dataset and MLlib computed polarity.
   */
  def saveAccuracy(sc: SparkContext, actualVsPredictionRDD: RDD[(Double, Double, String)]): Unit = {
    val sqlContext = SQLContextSingleton.getInstance(sc)
    import sqlContext.implicits._
    val actualVsPredictionDF = actualVsPredictionRDD.toDF("Actual", "Predicted", "Text")
    actualVsPredictionDF.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "\t")
      // Compression codec to compress while saving to file.
      .option("codec", classOf[GzipCodec].getCanonicalName)
      .mode(SaveMode.Append)
      //.save(Constants.modelAccuracyPath)
  }
}
