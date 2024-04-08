package dataApi
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
object DataApi {
  val route: Route =
    path("test") {
      get {
        complete("This is a test route in DataApi object.")
      }
    }

  path("home"){
    get {
      complete("This is home page.")
    }
  }

  path("airline"){
    get {
      complete("This is airline page.")
    }
  }

}