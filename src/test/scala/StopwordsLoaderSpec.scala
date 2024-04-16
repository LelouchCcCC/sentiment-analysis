import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import utils.StopwordsLoader

class StopwordsLoaderSpec extends AnyFlatSpec with Matchers {

  "loadStopWords" should "throw an IllegalStateException when the stopwords file is not found" in {
    an[IllegalStateException] should be thrownBy {
      StopwordsLoader.loadStopWords("nonexistent.txt")
    }
  }

  it should "successfully load stopwords from a valid file" in {
    val stopWords = StopwordsLoader.loadStopWords("validStopWords.txt")
    stopWords should not be empty
    stopWords should contain("and")
  }
}