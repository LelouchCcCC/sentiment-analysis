package dataApi
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import dataProcessor.DataProcessor.{read_new_csv}
import org.apache.spark.sql.SparkSession
class DataApi(spark: SparkSession) {
  val route: Route =
    path("airline") {
      println()
      get {
        val data = read_new_csv(spark)
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