import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class SentimentPredictTestSuite extends AnyFunSuite with DataFrameSuiteBase {

  import spark.implicits._

  test("loadData should load and filter data correctly") {
    val input = Seq(
      ("positive", "I love this airline."),
      ("negative", "I hate this airline."),
      ("neutral", "This airline is okay.")
    ).toDF("airline_sentiment", "text")

    input.createOrReplaceTempView("airline_data")

    def loadData(spark: SparkSession): DataFrame = {
      spark.sql("SELECT * FROM airline_data WHERE airline_sentiment rlike '^(neutral|positive|negative)$'")
    }

    val expected = Seq(
      ("positive", "I love this airline."),
      ("negative", "I hate this airline."),
      ("neutral", "This airline is okay.")
    ).toDF("airline_sentiment", "text")

    val result = loadData(spark)
    assertDataFrameEquals(expected, result)
  }

  test("calculateAccuracy should compute accuracy correctly") {
    val data = Seq(
      (1, 1),
      (1, 0),
      (1, 1),
      (0, 0)
    ).toDF("sentiment", "sentimentComputed")

    val testDf = data.withColumn("is_equal", when(col("sentiment") === col("sentimentComputed"), 1).otherwise(0))

    val accuracyResult = SentimentPredictTest.calculateAccuracy(testDf)
    assert(accuracyResult === 0.75)
  }
}