package dataApi

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import dataProcessor.DataProcessor.{read_new_csv, update_summary_csv}
import org.apache.spark.sql.SparkSession

import scala.util.Random
import utils.SentimentAnalyzer.singleAnalyze

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters._


class DataApi(spark: SparkSession) {
  val dateTimeFormat = DateTimeFormatter.ofPattern("M/d/yy HH:mm")
  //  val corsSettings = CorsSettings.defaultSettings.withAllowedOrigins(HttpOriginRange.*)

  val route: Route = {
    path("airline") {
      get {
        val data = read_new_csv(spark)
        complete(data)
      }
    }
  }

  val route2: Route = {
    path("oneSentiment") {
      get
      parameter("data") { sentiment_with_comma =>
        println(sentiment_with_comma)
        val sentiment = sentiment_with_comma.replace(',','.')
        val res = singleAnalyze(spark, sentiment).toString
        val newLine = createNewCsvLine("src/main/resources/data/airline.csv", res, sentiment)
        appendToCsv("src/main/resources/data/airline.csv", newLine)

        // use Future to compute the update of hourly data
        Future {
          update_summary_csv(spark)
        }
        complete(res)
        // should respond with -1/0/1
      }
    }
  }

  def getLastLine(filePath: String): String = {
    val lines = Files.readAllLines(Paths.get(filePath)).asScala
    lines.last
  }

  def createNewCsvLine(filePath: String, sentiment: String, text: String): String = {
    val lastLine = getLastLine(filePath)
    val lastParts = lastLine.split(",")
    val lastTimeStr = lastParts(5)
    val random = new Random()
    val randomUser = random.nextInt()
    val lastTime = LocalDateTime.parse(lastTimeStr, dateTimeFormat).plusMinutes(2)
    val newTimeStr = lastTime.format(dateTimeFormat)
    val airline_sentiment = sentiment match {
      case "-1" => "negative"
      case "0" => "neutral"
      case "1" => "positive"
    }

    // Assuming 'tweet_id' and 'airline_sentiment' are not provided
    s"$randomUser,$airline_sentiment,$sentiment,American,\"$text\",$newTimeStr"
  }

  def appendToCsv(filePath: String, newRow: String): Unit = {
    val file = new File(filePath)
    val writer = new BufferedWriter(new FileWriter(file, true))

    try {
      writer.newLine()
      writer.write(newRow)
    } finally {
      writer.close()
    }
  }

}