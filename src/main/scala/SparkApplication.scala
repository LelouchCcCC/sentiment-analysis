import org.apache.spark.sql.SparkSession
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import dataApi.DataApi

object SparkApplication extends App{

  val spark: SparkSession = SparkSession
    .builder()
    .appName("AirlineAnalysis")
    .master("local[*]") // Use local mode for development
    .getOrCreate()

  // Set log level to ERROR to reduce console output clutter
  spark.sparkContext.setLogLevel("ERROR")


  implicit val system: ActorSystem = ActorSystem("spark-application-system")
  implicit val executionContext = system.dispatcher

  val bindingFuture = Http().newServerAt("localhost", 8080).bind(DataApi.route)

  println("Server online at http://localhost:8080/")



}
