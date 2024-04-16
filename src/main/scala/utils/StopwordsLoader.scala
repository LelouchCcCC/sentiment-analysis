package utils

import scala.io.Source
import scala.util.Using

object StopwordsLoader {

  //  def loadStopWords(stopWordsFileName: String): List[String] = {
  //    Using(Thread.currentThread().getContextClassLoader.getResourceAsStream(stopWordsFileName)) { stream =>
  //      if (stream == null) {
  //        throw new IllegalStateException(s"Resource not found: $stopWordsFileName")
  //      }
  //      Source.fromInputStream(stream).getLines().toList
  //    }.getOrElse {
  //      throw new RuntimeException(s"Failed to load or parse the stop words file: $stopWordsFileName")
  //    }
  //  }

  def loadStopWords(stopWordsFileName: String): List[String] = {
    // 尝试从类路径加载文件资源
    val resourceStream = Option(Thread.currentThread().getContextClassLoader.getResourceAsStream(stopWordsFileName))
    resourceStream match {
      case Some(stream) =>
        // 自动管理资源，确保流正确关闭
        Using(stream) { inputStream =>
          Source.fromInputStream(inputStream).getLines().toList
        }.recover {
          case e: Exception => // 捕获解析时的具体异常
            throw new RuntimeException(s"Failed to parse the stop words file: $stopWordsFileName", e)
        }.get // get会抛出任何在Using块或recover中未处理的异常

      case None =>
        throw new IllegalStateException(s"Resource not found: $stopWordsFileName")
    }
  }

}