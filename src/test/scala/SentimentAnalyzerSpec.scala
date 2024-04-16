import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import utils.SentimentAnalyzer

import scala.util.control.NonFatal

class SentimentAnalyzerSpec extends AnyFlatSpec with Matchers {

  "SentimentAnalyzer" should "throw IllegalArgumentException if input text is null" in {
    val spark = SparkSession.builder().master("local[*]").appName("Sentiment Analysis Test").getOrCreate()
    assertThrows[IllegalArgumentException] {
      SentimentAnalyzer.singleAnalyze(spark, null)
    }
    spark.stop()
  }

  it should "throw IllegalStateException if stop words cannot be loaded" in {
    val spark = SparkSession.builder().master("local[*]").appName("Sentiment Analysis Test").getOrCreate()
    try {
      SentimentAnalyzer.singleAnalyze(spark, "This is a test.", "nonexistent.txt")
      fail("Should have thrown an IllegalStateException due to missing stop words")
    } catch {
      case e: IllegalStateException =>
      case NonFatal(ex) => fail("Unexpected exception type", ex)
    } finally {
      spark.stop()
    }
  }

  it should "return a sentiment score if everything is loaded correctly" in {
    val spark = SparkSession.builder().master("local[*]").appName("Sentiment Analysis Test").getOrCreate()
    try {
      val score = SentimentAnalyzer.singleAnalyze(spark, "I love Spark!")
      assert(score == 1 || score == 0 || score == -1)
    } finally {
      spark.stop()
    }
  }
}
