package dataApi

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import dataProcessor.DataProcessor.read_new_csv
import org.apache.spark.sql.SparkSession
import utils.SentimentAnalyzer.singleAnalyze

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters._


class DataApi(spark: SparkSession) {
  val dateTimeFormat = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm")
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
      parameter("data") { sentiment =>
        println(sentiment)
        val res = singleAnalyze(spark, sentiment).toString
        val newLine = createNewCsvLine("src/main/resources/data/airline.csv", res, sentiment)
        appendToCsv("src/main/resources/data/airline.csv", newLine)
        complete(res)
        // should respond with 这句话的好坏-1/0/1
        // 还要处理一下时间相关的东西，更新一下new_csv
      }


      //      get {
      //        parameter("data") { sentimentData =>
      //          println(s"Received sentiment data: $sentimentData")
      //          // 这里调用您的情感分析逻辑
      //          val res = analyzeSentiment(sentimentData)
      //
      //          // 处理CSV更新操作，如果需要的话
      //          updateCsvIfNeeded(spark)
      //
      //          // 根据分析结果返回响应
      //          complete(StatusCodes.OK, s"情感分析结果: $res")
      //        }
      //      }
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
    val lastTime = LocalDateTime.parse(lastTimeStr, dateTimeFormat).plusMinutes(2)
    val newTimeStr = lastTime.format(dateTimeFormat)

    // Assuming 'tweet_id' and 'airline_sentiment' are not provided
    s",,$sentiment,,\"$text\",$newTimeStr"
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