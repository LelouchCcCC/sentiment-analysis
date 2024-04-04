import org.apache.spark.sql.SparkSession

object SparkSQLiteExample extends App {
  val spark = SparkSession.builder()
    .appName("Spark SQLite Example")
    .master("local[*]") // 使用本地模式运行
    .getOrCreate()

  // SQLite 数据库文件路径
  val dbPath = "src/main/resources/database/database.sqlite"
  val dbUrl = s"jdbc:sqlite:$dbPath"

  // 数据库表名
  val tableName = "Tweets"

  // 读取数据表
  val df = spark.read
    .format("jdbc")
    .option("url", dbUrl)
    .option("dbtable", tableName)
    .load()

  df.show() // 显示数据帧内容
}