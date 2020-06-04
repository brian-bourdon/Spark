import org.apache.spark.sql.SparkSession

object FootballApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    println("hello")
    spark.stop()
  }
}