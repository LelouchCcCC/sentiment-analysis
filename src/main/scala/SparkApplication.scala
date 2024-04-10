import org.apache.spark.sql.SparkSession
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import dataApi.DataApi

/**
 * Used to set up the back end server
 */

object SparkApplication extends App{

  val spark: SparkSession = SparkSession
    .builder()
    .appName("AirlineAnalysis")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .master("local[*]") // Use local mode for development
    .getOrCreate()

  // Set log level to ERROR to reduce console output clutter
  spark.sparkContext.setLogLevel("ERROR")


  implicit val system: ActorSystem = ActorSystem("spark-application-system")
  implicit val executionContext = system.dispatcher

  val dataApi = new DataApi(spark)
  val bindingFuture = Http().newServerAt("localhost", 8080).bind(dataApi.route)

  println("Server online at http://localhost:8080/")

}
