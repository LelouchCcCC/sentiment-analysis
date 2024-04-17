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

    df.na.drop()

  }

  def changeTime(df: DataFrame, spark: SparkSession): DataFrame = {
    df.createOrReplaceTempView("tweets")

    // get the date of yesterday
    val yesterday = LocalDate.now(ZoneId.systemDefault()).minusDays(1)

    // adjust the date
    val query =
      s"""
      SELECT *,
             to_timestamp(concat(date_format(to_date('$yesterday'), 'yyyy-MM-dd'), ' ', date_format(tweet_created, 'HH:mm:ss'))) as adjusted_tweet_created
      FROM tweets
    """
    val adjustedDf = spark.sql(query)

    adjustedDf.drop("tweet_created").withColumnRenamed("adjusted_tweet_created", "tweet_created")
  }


  // this method is a copy to zzh method
  def retn_data(spark:SparkSession) = {
    val sc = spark.sparkContext

    // load stop words
    val stopWordsList = sc.broadcast(StopwordsLoader.loadStopWords(Constants.STOP_WORDS))

    val airlineData = spark.read.option("inferSchema", "true").option("header", "true").csv("src/main/resources/data/airline.csv")

    // load model
    val modelPath = Constants.naiveBayesModelPath
    val naiveBayesModel = NaiveBayesModel.load(sc, modelPath)

    val filteredAirlineData = airlineData.filter(col("airline_sentiment").rlike("^(neutral|positive|negative)$"))
    val dropNAAirline = filteredAirlineData.na.drop()

    val processText = udf((s: String) => computeSentiment(s, stopWordsList, naiveBayesModel))
    val modifiedDf = dropNAAirline.withColumn("sentimentComputed", processText(col("text")))
    modifiedDf.show(-400)

    val dfWithInt = modifiedDf.withColumn("sentiment", col("sentiment").cast("int"))
    val comparisonDf = dfWithInt.withColumn("is_equal", when(col("sentiment") === col("sentimentComputed"), 1).otherwise(0))

    val accuracy = comparisonDf.agg(sum("is_equal").cast("double") / count("is_equal")).first().get(0).asInstanceOf[Double]

    println(s"Accuracy: $accuracy")

    // change tweet_created into date type
    val dfWithTimestamp = comparisonDf.withColumn("tweet_timestamp", unix_timestamp(col("tweet_created"), "M/d/yy H:mm").cast(TimestampType))

    // extract the hour
    val dfWithHour = dfWithTimestamp.withColumn("hour", hour(col("tweet_timestamp")))

    // group by hour and calculate the avg of sentiment values and computed sentiment values
    val averageSentimentByHour = dfWithHour.groupBy("hour")
      .agg(
        round(avg("sentiment"), 2).alias("average_sentiment"),
        round(avg("sentimentComputed"), 2).alias("average_sentiment_computed")
      )
      .orderBy("hour")

    // show result
    averageSentimentByHour.show()

    val resultDf = averageSentimentByHour
      .withColumn("abs_difference", round(abs(col("average_sentiment") - col("average_sentiment_computed")), 2))
      .withColumn("squared_difference", round(pow(col("average_sentiment") - col("average_sentiment_computed"), 2), 2))


    resultDf.show()

    val jsonStrings = resultDf.toJSON.collect().mkString("[", ",", "]")
    jsonStrings
  }

  def read_new_csv(spark:SparkSession) = {
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Constants.NEW_CSV_FILE_FOLDER)
    df.show()
    val jsonStrings = df.toJSON.collect().mkString("[", ",", "]")
    jsonStrings

  }

  def update_summary_csv(spark:SparkSession) = {
    val sc = spark.sparkContext

    // read stop_words
    val stopWordsList = sc.broadcast(StopwordsLoader.loadStopWords(Constants.STOP_WORDS))

    val airlineData = spark.read.option("inferSchema", "true").option("header", "true").csv("src/main/resources/data/airline.csv")

    // load the model
    val modelPath = Constants.naiveBayesModelPath
    val naiveBayesModel = NaiveBayesModel.load(sc, modelPath)

    val filteredAirlineData = airlineData.filter(col("airline_sentiment").rlike("^(neutral|positive|negative)$"))
    val dropNAAirline = filteredAirlineData.na.drop()

    val processText = udf((s: String) => computeSentiment(s, stopWordsList, naiveBayesModel))
    val modifiedDf = dropNAAirline.withColumn("sentimentComputed", processText(col("text")))
    val totalRows = modifiedDf.count()
    val lastRows = 10
    val skip = totalRows - lastRows

    println(s"Total counts: $totalRows")
    val tailDf = modifiedDf.tail(lastRows)
    tailDf.foreach(println)
    // create a new column, if the two columns are the same, put 1 in there else 0
    val dfWithInt = modifiedDf.withColumn("sentiment", col("sentiment").cast("int"))
    val comparisonDf = dfWithInt.withColumn("is_equal", when(col("sentiment") === col("sentimentComputed"), 1).otherwise(0))

    // calculate the accuracy
    val accuracy = comparisonDf.agg(sum("is_equal").cast("double") / count("is_equal")).first().get(0).asInstanceOf[Double]

    println(s"Accuracy: $accuracy")

    // convert date type of tweet_created
    val dfWithTimestamp = comparisonDf.withColumn(
      "tweet_timestamp",
      unix_timestamp(col("tweet_created"), "M/d/yy H:mm").cast(TimestampType)
    )
    // get the hour, as we do the group analysisgroup
    val dfWithHour = dfWithTimestamp.withColumn("hour", hour(col("tweet_timestamp")))
    val dfWithDateHour = dfWithTimestamp.withColumn(
      "date",
      date_format(col("tweet_timestamp"), "yyyy-MM-dd")
    ).withColumn(
      "hour",
      hour(col("tweet_timestamp"))
    )

    val averageSentimentByDateHour = dfWithDateHour.groupBy("date", "hour")
      .agg(
        count("*").alias("count"),
        round(avg("sentiment"), 2).alias("average_sentiment"),
        round(avg("sentimentComputed"), 2).alias("average_sentiment_computed")
      )
      .orderBy("date", "hour")

    // show result
    averageSentimentByDateHour.show(30)

    val resultDf = averageSentimentByDateHour
      .withColumn("abs_difference", round(abs(col("average_sentiment") - col("average_sentiment_computed")), 2))
      .withColumn("squared_difference", round(pow(col("average_sentiment") - col("average_sentiment_computed"), 2), 2))


    resultDf.show(30)

    val outputPath = "src/main/resources/output/average_sentiment_by_hour.csv"

    // store dataframe as csv
    resultDf
      .coalesce(1)
      .write
      .option("header", "true") 
      .mode("overwrite")
      .csv(outputPath)
  }
}
