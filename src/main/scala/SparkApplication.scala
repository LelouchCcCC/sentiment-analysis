import org.apache.spark.sql.SparkSession
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.ExecutionDirectives._
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import dataApi.DataApi

/**
 * Used to set up the back end server
 */

object SparkApplication extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("AirlineAnalysis")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  implicit val system: ActorSystem = ActorSystem("spark-application-system")
  implicit val executionContext = system.dispatcher

  val dataApi = new DataApi(spark)

  // 封装你的路由以启用CORS
  val corsEnabledRoute = cors() {
    dataApi.route
  }

  val bindingFuture = Http().newServerAt("localhost", 8080).bind(corsEnabledRoute)

  println("Server online at http://localhost:8080/")
}