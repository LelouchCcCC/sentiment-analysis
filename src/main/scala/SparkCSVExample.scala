import dataProcessor.DataProcessor
import org.apache.spark.sql.SparkSession

object AirlineAnalysis extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("AirlineAnalysis")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .master("local[*]") // Use local mode for development
    .getOrCreate()

  // Set log level to ERROR to reduce console output clutter
  spark.sparkContext.setLogLevel("ERROR")

  // Define the path to the CSV file
  val filename = "src/main/resources/data/airline.csv"

  // Load the CSV file
  val df = DataProcessor.loadData(spark,filename)


  df.show(20)


  // Stop the SparkSession
  spark.stop()
}
