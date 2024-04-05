import dataProcessor.DataProcessor
import org.apache.spark.sql.SparkSession

object AirlineAnalysis extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("AirlineAnalysis")
    .master("local[*]") // Use local mode for development
    .getOrCreate()

  // Set log level to ERROR to reduce console output clutter
  spark.sparkContext.setLogLevel("ERROR")

  // Define the path to the CSV file
  val filename = "src/main/resources/data/airline.csv"

  // Load the CSV file
  val df = DataProcessor.loadData(spark,filename)

  val changedTimeDF = DataProcessor.changeTime(df,spark)

  changedTimeDF.show(20)

  // Assuming the column containing ratings is named "rating"
  val ratings = df.select("airline_sentiment_confidence").na.drop() // Drop rows with null ratings

  // Calculate mean and standard deviation
  val stats = ratings.describe("airline_sentiment_confidence")

  // Show the result
  stats.show()

  // Stop the SparkSession
  spark.stop()
}
