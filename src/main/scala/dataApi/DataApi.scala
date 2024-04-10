package dataApi
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import dataProcessor.DataProcessor.retn_data
import org.apache.spark.sql.SparkSession
class DataApi(spark: SparkSession) {
  val route: Route =
    path("airline") {
      println()
      get {
        val data = retn_data(spark)
        complete(data)
      }
    }

//
//  path("airline"){
//    get {
//      complete("This is airline page.")
//    }
//  }



}