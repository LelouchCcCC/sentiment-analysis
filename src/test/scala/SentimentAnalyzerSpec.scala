import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import utils.SentimentAnalyzer
import scala.util.control.NonFatal

class SentimentAnalyzerSpec extends AnyFlatSpec with Matchers with DataFrameSuiteBase {

  "SentimentAnalyzer" should "throw IllegalArgumentException if input text is null" in {
    intercept[IllegalArgumentException] {
      SentimentAnalyzer.singleAnalyze(spark, null)
    }
  }

  it should "throw IllegalStateException if stop words cannot be loaded" in {
    intercept[IllegalStateException] {
      SentimentAnalyzer.singleAnalyze(spark, "This is a test.", "nonexistent.txt")
    }
  }

  it should "return a sentiment score if everything is loaded correctly" in {
    val score = SentimentAnalyzer.singleAnalyze(spark, "I love Spark!")
    assert(score == 1 || score == 0 || score == -1)
  }
}