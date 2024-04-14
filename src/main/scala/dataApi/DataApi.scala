package dataApi

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import dataProcessor.DataProcessor.read_new_csv
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.sql.SparkSession
import utils.SentimentAnalyzer.singleAnalyze
import utils.{Constants, SentimentAnalyzer, StopwordsLoader}

class DataApi(spark: SparkSession) {

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
            parameter("data"){ sentiment =>
              println(sentiment)
              val res = singleAnalyze(spark, sentiment).toString
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


}