import org.apache.spark.sql.SparkSession

object SparkScalaExample {
  def main(args: Array[String]): Unit = {
    // 初始化 SparkSession
    val spark = SparkSession.builder()
      .appName("Spark Scala Example")
      .config("spark.master", "local")
      .getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 创建一个简单的 DataFrame
    val data = Seq(("James", "Smith"), ("Michael", "Rose"), ("Robert", "Williams"))
    val df = data.toDF("firstname", "lastname")

    // 显示 DataFrame
    df.show()

    // 使用 SQL 语句处理数据
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people WHERE firstname = 'Michael'")
    sqlDF.show()

    // 停止 SparkSession
    spark.stop()
  }
}
