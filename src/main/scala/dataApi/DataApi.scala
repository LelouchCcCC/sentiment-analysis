package dataApi
import akka.http.scaladsl.model.headers.HttpOriginRange
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.megard.akka.http.cors.javadsl.settings.CorsSettings
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import dataProcessor.DataProcessor.read_new_csv
import org.apache.spark.sql.SparkSession
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
        val res = ???

        complete(res)
        // should respond with 这句话的好坏-1/0/1
        // 还要处理一下时间相关的东西，更新一下new_csv
      }
    }
  }





}