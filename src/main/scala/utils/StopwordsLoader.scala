package utils

import scala.io.Source
import scala.util.Using

object StopwordsLoader {

  def loadStopWords(stopWordsFileName: String): List[String] = {
    val resourceStream = Option(Thread.currentThread().getContextClassLoader.getResourceAsStream(stopWordsFileName))
    resourceStream match {
      case Some(stream) =>
        Using(stream) { inputStream =>
          Source.fromInputStream(inputStream).getLines().toList
        }.recover {
          case e: Exception =>
            throw new RuntimeException(s"Failed to parse the stop words file: $stopWordsFileName", e)
        }.get

      case None =>
        throw new IllegalStateException(s"Resource not found: $stopWordsFileName")
    }
  }

}