package utils

import scala.io.Source

object StopwordsLoader {

  def loadStopWords(stopWordsFileName: String): List[String] = {
    Source.fromInputStream(getClass.getResourceAsStream("/" + stopWordsFileName)).getLines().toList
  }
}